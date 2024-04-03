from constants import RESOURCE_KEY, \
    OBSERVATION_SET_KEY, \
    FEATURE_SET_KEY, \
    PARENT_OP_KEY, \
    UNSPECIFIED_FORMAT


class DataResource(object):

    STANDARD_FORMAT = UNSPECIFIED_FORMAT

    def validate_type(self, resource_instance, file_format):
        raise NotImplementedError('You must'
        ' implement this method in the derived class')

    def performs_validation(self):
        '''
        Certain types of files (e.g. fastq) are laborious
        to validate or we cannot reliably check those. To 
        prevent localization of large DataResources that skip
        validation, we expose this method.

        By default, return False, which asserts that the resource
        type does not implement validation methods. It is the job
        of the child classes to implement this method if they are
        able to perform validation
        '''
        return False


    def get_contents(self, resource_instance, query_params={}, preview=False):
        raise NotImplementedError('You must'
        ' implement this method in the derived class')

    def extract_metadata(self, resource_instance, parent_op_pk=None):
        raise NotImplementedError('You must'
        ' implement this method in the derived class')

    def to_json(self, content):
        raise NotImplementedError('You must'
        ' implement this method in the derived class')
        
    @staticmethod
    def get_file_extension(path):
        '''
        A single method to return the extension of the file. By convention,
        the lower-cased contents AFTER the final dot/period.
        '''
        return path.split('.')[-1].lower()

    @staticmethod
    def get_paginator():
        raise NotImplementedError('Must override this method in a subclass.')

    def setup_metadata(self):
        '''
        This sets up the basic dict that will eventually be submitted
        to the ResourceMetadata deserializer.  Child classes will
        fill-in the fields as appropriate.
        '''
        self.metadata = {
            PARENT_OP_KEY: None,
            OBSERVATION_SET_KEY: None,
            FEATURE_SET_KEY: None,
            RESOURCE_KEY: None
        }

    def save_in_standardized_format(self, resource_path, file_format):
        '''
        This method is used for saving user-supplied resources/files as something
        we can consistently refer to. For instance, users may load csv, tsv, excel, etc.
        files for data. In the analyses, we don't want to constantly be checking for
        how to parse these types. Hence, for table-based resources, we simply rewrite the
        file as a TSV so that all analyses can safely assume they will be given a valid
        TSV-format file.

        In this base-class implementation, we don't do anything- just echo back.
        '''
        return resource_path

