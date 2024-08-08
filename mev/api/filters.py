import operator
import numpy as np

# we allow sorting of the resource contents (if sensible for the resource)
SORT_PARAM = 'sort_vals'
ASCENDING = '[asc]'
DESCENDING = '[desc]'
SORTING_OPTIONS = [ASCENDING, DESCENDING]

# A special filter option for specifying a filter on the row names
ROWNAME_FILTER = '__rowname__'

# A special filter option for specifying a filter on the columns
COLNAME_FILTER = '__colname__'

# for query filter params:
# When providing query filters, we will have something like:
# <url>/?paramA=<comparison>:<val>, 
# /e.g. <url>/?pval=[lte]:0.01?log2FoldChange=[gte]:2
# which will filter for pval <= 0.01 and log fold change >= 2
# The delimiter between the comparison and the value is given below:
QUERY_PARAM_DELIMITER = ':'
LESS_THAN = '[lt]'
LESS_THAN_OR_EQUAL = '[lte]'
GREATER_THAN = '[gt]'
GREATER_THAN_OR_EQUAL = '[gte]'
ABS_VAL_GREATER_THAN = '[absgt]'
ABS_VAL_LESS_THAN = '[abslt]'
EQUAL_TO = '[eq]'
CASE_INSENSITIVE_EQUALS = '[case-ins-eq]'
STARTSWITH = '[startswith]'
IS_IN = '[in]'

def is_valid_numerical_comparison(x,y):
    try:
        return True, float(x), float(y)
    except ValueError as ex:
        return False, x, y
    except TypeError as ex:
        return False, x, y

def abs_val_gt(x,y):
    valid, x, y = is_valid_numerical_comparison(x,y)
    if valid:
        return np.abs(x) > y
    else:
        return False

def abs_val_lt(x,y):
    valid, x, y = is_valid_numerical_comparison(x,y)
    if valid:
        return np.abs(x) < y
    else:
        return False

def case_insensitive_string_compare(x,y):
    return x.lower() == y.lower()

def case_insensitive_startswith(x,y):
    return x.lower().startswith(y.lower())

def list_contains(x,y):
    # y is a comma-delimited string of identifiers to find
    y_list = [a.strip() for a in y.split(',')]
    return x in y_list

def lte(x,y):
    valid, x, y = is_valid_numerical_comparison(x,y)
    if valid:
        return x<=y
    else:
        return False

def gte(x,y):
    valid, x, y = is_valid_numerical_comparison(x,y)
    if valid:
        return x>=y
    return False

def lt(x,y):
    valid, x, y = is_valid_numerical_comparison(x,y)
    if valid:
        return x<y
    return False

def gt(x,y):
    valid, x, y = is_valid_numerical_comparison(x,y)
    if valid:
        return x>y
    return False

OPERATOR_MAPPING = {
    LESS_THAN: lt,
    LESS_THAN_OR_EQUAL: lte,
    GREATER_THAN: gt,
    GREATER_THAN_OR_EQUAL: gte,
    ABS_VAL_GREATER_THAN: abs_val_gt,
    ABS_VAL_LESS_THAN: abs_val_lt,
    EQUAL_TO: operator.eq,
    '=': operator.eq, # add for convenience
    '==': operator.eq, # add for convenience
    CASE_INSENSITIVE_EQUALS: case_insensitive_string_compare,
    STARTSWITH: case_insensitive_startswith,
    IS_IN: list_contains
}

NUMERIC_OPERATORS = [
    LESS_THAN,
    LESS_THAN_OR_EQUAL,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL,
    ABS_VAL_GREATER_THAN,
    ABS_VAL_LESS_THAN,
]

# used for translating the GET query params (like "[lte]")
# into SQL-compatible strings (like "<=" for "[lte]")
NUMERIC_OPERATOR_SYMBOLS = {
    LESS_THAN: '<',
    LESS_THAN_OR_EQUAL: '<=',
    GREATER_THAN: '>',
    GREATER_THAN_OR_EQUAL: '>='
}

