class StringIdentifierException(Exception):
    '''
    Raised if a String identifier (e.g. an observation "name")
    does not match our constraints.  See the utilities method
    for those expectations/constraints.
    '''
    pass


class AttributeValueError(Exception):
    '''
    Raised by the attribute subclasses if something is amiss.
    For example, if we try to create an IntegerAttribute with
    a string.
    '''
    pass


class InvalidAttributeKeywords(Exception):
    '''
    Raised if invalid keyword args are passed to the constructor of
    an Attribute subclass
    '''
    pass


class InputMappingException(Exception):
    '''
    Raised if there is an exception to be raised when mapping a user's 
    inputs to job inputs when an ExecutedOperation has been requested.
    '''
    pass


class OperationResourceFileException(Exception):
    '''
    This exception is raised if an Operation specifies user-indepdent
    files that are associated with the Operation, but there is an issue
    finding or reading the file. 
    Raised during the ingestion of the operation
    '''
    pass

class NoResourceFoundException(Exception):
    '''
    Raised as a general exception when a Resource cannot be found.
    '''
    pass