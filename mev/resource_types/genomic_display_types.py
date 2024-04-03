from constants import PARENT_OP_KEY, \
    UNSPECIFIED_FORMAT

from .base import DataResource


class WigFileResource(DataResource):

    ACCEPTABLE_FORMATS = [UNSPECIFIED_FORMAT]
    DESCRIPTION = 'A WIG-format file.'

    def validate_type(self, resource_instance, file_format):
        '''
        Rather than validate these files, we will depend on tools
        that use them to reject poor formatting
        '''
        pass

    def extract_metadata(self, resource_instance, parent_op_pk=None):
        # call the super method to initialize the self.metadata
        # dictionary
        super().setup_metadata()

        # now add the information to self.metadata:
        if parent_op_pk:
            self.metadata[PARENT_OP_KEY] = parent_op_pk
        return self.metadata

    def get_contents(self, resource_instance, query_params={}, preview=False):
        return None


class BigWigFileResource(DataResource):

    ACCEPTABLE_FORMATS = [UNSPECIFIED_FORMAT]
    DESCRIPTION = 'A BigWIG-format file.'

    def validate_type(self, resource_instance, file_format):
        '''
        Rather than validate these files, we will depend on tools
        that use them to reject poor formatting
        '''
        pass

    def extract_metadata(self, resource_instance, parent_op_pk=None):
        # call the super method to initialize the self.metadata
        # dictionary
        super().setup_metadata()

        # now add the information to self.metadata:
        if parent_op_pk:
            self.metadata[PARENT_OP_KEY] = parent_op_pk
        return self.metadata

    def get_contents(self, resource_instance, query_params={}, preview=False):
        return None


class BedGraphFileResource(DataResource):

    ACCEPTABLE_FORMATS = [UNSPECIFIED_FORMAT]
    DESCRIPTION = 'A BedGraph-format file.'

    def validate_type(self, resource_instance, file_format):
        '''
        Rather than validate these files, we will depend on tools
        that use them to reject poor formatting
        '''
        pass

    def extract_metadata(self, resource_instance, parent_op_pk=None):
        # call the super method to initialize the self.metadata
        # dictionary
        super().setup_metadata()

        # now add the information to self.metadata:
        if parent_op_pk:
            self.metadata[PARENT_OP_KEY] = parent_op_pk
        return self.metadata

    def get_contents(self, resource_instance, query_params={}, preview=False):
        return None