# This file contains information about the different 
# sequence-based file types and methods for validating them

import logging

from constants import FASTQ_FORMAT, \
    FASTA_FORMAT, \
    BAM_FORMAT, \
    PARENT_OP_KEY, \
    UNSPECIFIED_FORMAT

from .base import DataResource

logger = logging.getLogger(__name__)


class SequenceResource(DataResource):
    '''
    This class is used to represent sequence-based files
    such as Fasta, Fastq, SAM/BAM

    We cannot (reasonably) locally validate the contents of these 
    files quickly or exhaustively, so minimal validation is performed
    remotely
    '''
    @classmethod
    def validate_type(cls, resource_instance, file_format):
        pass

    def extract_metadata(self, resource_instance, parent_op_pk=None):
        '''
        For sequence-based types, we implement a trivial metadata
        extraction, as these resource types are not typically amenable
        to fast/easy parsing (possibly files that are many GB)

        Fill out an basic metadata object
        '''
        logger.info('Extracting metadata from resource'
            f' {resource_instance.pk}')

        # call the super method to initialize the self.metadata
        # dictionary
        super().setup_metadata()

        # now add the information to self.metadata:
        if parent_op_pk:
            self.metadata[PARENT_OP_KEY] = parent_op_pk
        return self.metadata

    def get_contents(self, resource_instance, query_params={}, preview=False):
        '''
        By default, we do not view sequence types via the API, so return `None`
        '''
        return None

class FastAResource(SequenceResource):
    '''
    This type is for compressed Fasta files
    '''
    DESCRIPTION = 'FASTA-format sequence file.'

    ACCEPTABLE_FORMATS = [
        FASTA_FORMAT
    ]
    STANDARD_FORMAT = FASTA_FORMAT

    def validate_type(self, resource_instance, file_format):
        pass

class FastQResource(SequenceResource):
    '''
    This resource type is for gzip-compressed Fastq files
    '''
    DESCRIPTION = 'FASTQ-format sequence file.  The most common format'\
        ' used for sequencing experiments. Should be GZIP compressed'\
        ' which is typically denoted with a "fastq.gz" file extension.'

    ACCEPTABLE_FORMATS = [
        FASTQ_FORMAT
    ]
    STANDARD_FORMAT = FASTQ_FORMAT

    def validate_type(self, resource_instance, file_format):
        pass


class AlignedSequenceResource(SequenceResource):
    '''
    This resource type is for SAM/BAM files. 
    '''
    DESCRIPTION = 'BAM-format aligned sequence files.  Typically the' \
        ' output of an alignment process.'


    ACCEPTABLE_FORMATS = [
        BAM_FORMAT
    ]
    STANDARD_FORMAT = BAM_FORMAT

    def validate_type(self, resource_instance, file_format):
        pass


class BAMIndexResource(SequenceResource):
    '''
    This is used to identify index files for BAMs. 
    '''
    DESCRIPTION = 'Index file corresponding to a BAM-format aligned sequence file.'

    ACCEPTABLE_FORMATS = [UNSPECIFIED_FORMAT]

    def validate_type(self, resource_instance, file_format):
        pass