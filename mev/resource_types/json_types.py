import json
import logging
from collections import OrderedDict
import numpy as np

from django.core.paginator import Paginator, Page
from django.conf import settings

from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response

from constants import JSON_FORMAT, \
    PARENT_OP_KEY

from exceptions import NonIterableContentsException, \
    ParseException

from api.utilities.admin_utils import alert_admins

from .base import DataResource

logger = logging.getLogger(__name__)


class JsonArrayPage(Page):
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
        return self.object_list[index]        


class JsonObjectPage(Page):
    def __getitem__(self, index):
        keys = list(self.object_list.keys())
        #self.object_list = list(self.object_list)
        return self.object_list[keys[index]]


class JsonResourcePaginator(Paginator):
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
    def page(self, number):
        """Return a Page object for the given 1-based page number."""
        number = self.validate_number(number)
        bottom = (number - 1) * self.per_page
        top = bottom + self.per_page
        if top + self.orphans >= self.count:
            top = self.count
        if type(self.object_list) == list:
            return JsonArrayPage(self.object_list[bottom:top], number, self)
        else:
            raise NonIterableContentsException()


class JsonResourcePageNumberPagination(PageNumberPagination):
    django_paginator_class = JsonResourcePaginator
    page_size_query_param = settings.PAGE_SIZE_PARAM

    def get_paginated_response(self, data):
        return Response(OrderedDict([
            ('count', self.page.paginator.count),
            ('next', self.get_next_link()),
            ('previous', self.get_previous_link()),
            ('results', data)
        ]))


def create_closure(op, filter_val):
    def f(x):
        return op(x, filter_val)
    return f

class JsonResource(DataResource):

    STANDARD_FORMAT = JSON_FORMAT

    ACCEPTABLE_FORMATS = [JSON_FORMAT]
    DESCRIPTION = 'A JSON-format file.'

    @staticmethod
    def get_paginator():
        return JsonResourcePageNumberPagination()

    def to_json(self, content):
        '''
        Since this resource class already handles JSON
        data, this is trivial.
        '''
        return content

    def performs_validation(self):
        '''
        Since we have methods to validate JSON-based DataResource types, we 
        override this method and return True, which indicates that we CAN
        perform validation.
        '''
        return True

    def validate_type(self, resource_instance, file_format):

        try:
            logger.info('Using python-native JSON loader to read'
                f' resource: {resource_instance.pk}')
            j = json.load(resource_instance.datafile.open())
            logger.info(f'Successfully parsed {resource_instance.pk} as JSON.')
            return (True, None)
        except json.decoder.JSONDecodeError as ex:
            logger.info('Failed to parse JSON-based resource.')
            return (False, 'There was an issue with the JSON formatting.'
                f' The reported error was: {ex}')
        except Exception as ex:
            logger.info('Parsing a JSON resource raised an unexpected'
                f' exception. Exception was: {ex}')
            return (False, 'There was an unexpected encountered when attempting'
                f' to parse the file was JSON. The reported error was: {ex}')

    def extract_metadata(self, resource_instance, parent_op_pk=None):
        # call the super method to initialize the self.metadata
        # dictionary
        super().setup_metadata()

        # now add the information to self.metadata:
        if parent_op_pk:
            self.metadata[PARENT_OP_KEY] = parent_op_pk
        return self.metadata

    def get_contents(self, resource_instance, query_params={}, preview=False):

        # since the pagination query params are among the general query parameters, we DON'T
        # want to pass them to the filtering.
        filtering_query_params = {}
        ignored_params = [settings.PAGE_SIZE_PARAM, settings.PAGE_PARAM, settings.SORT_PARAM]
        for k,v in query_params.items():
            if (not k in ignored_params):
                filtering_query_params[k] = v

        logger.info('Get contents of JSON resource and filter'
            f' against query params: {filtering_query_params}')
        try:
            logger.info('Using python-native JSON loader to'
                f' read resource: {resource_instance.pk}')
            j = json.load(resource_instance.datafile.open())
            if filtering_query_params:
                j = self.filter_based_on_query_params(j, filtering_query_params)
            if settings.SORT_PARAM in query_params:
                j = self.sort_json(j, query_params[settings.SORT_PARAM])
            return j
        except ParseException as ex:
            raise ex
        except Exception as ex:
            logger.info(f'Failed to load JSON resource. Error was {ex}')
            raise ex

    def sort_json(self, j, sort_string):
        '''
        Return the results sorted by a particular field.
        Only permit simple sorts. No nested sorts on multiple
        fields
        '''
        if len(sort_string.split(',')) > 1:
            raise ParseException(f'Based on the query string ({sort_string})'
                ' it appeared that a sort on multiple fields was requested.'
                ' For JSON-based arrays, only sorts on single fields are'
                ' permitted.')

        try:
            sort_order, field = sort_string.split(settings.QUERY_PARAM_DELIMITER)
        except ValueError:
            raise ParseException('The sorting request was not properly formatted. '
                'Please use "<ordering keyword>:<column name>"')
                        
        if not sort_order in settings.SORTING_OPTIONS:
            raise ParseException(f'The sort order "{sort_order}" is'
                ' not an available option. Choose'
                f' from: {",".join(settings.SORTING_OPTIONS)}')

        # extract the values for the field of interest. If that field doesn't exist
        # on the item, then assign it to np.nan
        current_vals = []
        num_nans = 0 # track which indexes are NaN
        for i,item in enumerate(j):
            try:
                current_vals.append(item[field])
            except KeyError:
                current_vals.append(np.nan)
                num_nans += 1

        # now use argsort to get the ordering. There is no way to 
        # specify asc/descending as it always makes it ascending.
        # Note that the np.nan get sent to the end.
        ordering = np.argsort(current_vals)
        
        if sort_order ==  settings.DESCENDING:
            if num_nans > 0:
                nan_idx = ordering[-num_nans:]
                # We strip off the indexes corresponding to the NaNs
                # so that we can reverse the list and not put those NaNs at the front            
                ordering = ordering[:-num_nans][::-1]
                ordering.extend(nan_idx) # then add them back onto the tail
            else:
                ordering = ordering[::-1]
        return [j[k] for k in ordering]


    def filter_based_on_query_params(self, j, query_params):
        # we can only really filter if the json data structure is list-like:
        if not type(j) is list:
            return j

        filter_ops = {}
        for k,v in query_params.items():
            # v is either a value (in the case of strict equality)
            # or a delimited string which will dictate the comparison.
            # For example, to filter on the 'pval' column for values less than or equal to 0.01, 
            # v would be "[lte]:0.01". The "[lte]" string is set in our general settings file.
            split_v = v.split(settings.QUERY_PARAM_DELIMITER)
            if len(split_v) == 1: # strict equality filter
                # the query could be asking for a strict equality of a number or string
                # Try to cast as a number. If it fails, we assume it's a string filter
                try:
                    val = float(v)
                except ValueError as ex:
                    val = v
                
                filter_ops[k] = create_closure(settings.OPERATOR_MAPPING['=='], val)

            elif len(split_v) == 2:
                # something like "[lte]:0.01" or "[startswith]:aaa"
                # Can be numeric comparisons or string. 
                val = split_v[1] # e.g. "0.01" (still a string!) or "aaa" if doing a string comparison

                # If we were given a strictly numeric-style comparison (e.g. <=)
                # then we immediately fail the request if `val` was not a number
                op_id = split_v[0]
                if op_id in settings.NUMERIC_OPERATORS:
                    try:
                        val = float(val)
                    except ValueError as ex:
                        raise ParseException('Could not interpret the query'
                            f' parameter value {val} as a number.')

                # the supplied value was ok. Check the operator supplied
                try:
                    op = settings.OPERATOR_MAPPING[op_id]
                except KeyError as ex:
                    raise ParseException(f'The operator string ("{op_id}")'
                        ' was not understood. Choose from among:'
                        f' {",".join(settings.OPERATOR_MAPPING.keys())}')
                filter_ops[k] = create_closure(op, val)

            else:
                raise ParseException(f'The query param string ({v}) for'
                    f' filtering on the "{k}" field was not'
                    ' formatted properly.')
        # now go through the list and keep those that pass the filter
        filtered_list = []
        for item in j:
            tests = []
            for k, op in filter_ops.items():
                try:
                    try:
                        tests.append(op(item[k]))
                    except Exception as ex:
                        logger.info('Error with comparison for'
                            ' filtering a JSON resource contents.'
                        )
                        raise ex
                except KeyError as ex:
                    # the key was not in the item. We don't consider this as an error
                    tests.append(False)
            if all(tests):
                filtered_list.append(item)
        return filtered_list

    def save_to_file(self, contents, file_handle):
        '''
        Write the table-like resource to `filepath`
        '''
        try:
            file_handle.write(json.dumps(contents))

        except Exception as ex:
            err_msg = (f'Failed to write JSON resource.'
                      f' Reason was {ex}.')
            logger.error(err_msg)
            alert_admins(err_msg)
            raise ex