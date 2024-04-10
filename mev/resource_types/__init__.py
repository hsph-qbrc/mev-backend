import logging

from constants import FASTQ_KEY, \
    FASTA_KEY, \
    ALIGNMENTS_KEY, \
    BAI_KEY, \
    FEATURE_TABLE_KEY, \
    MATRIX_KEY, \
    INTEGER_MATRIX_KEY, \
    EXPRESSION_MATRIX_KEY, \
    RNASEQ_COUNT_MATRIX_KEY, \
    NETWORK_DESCRIPTOR_KEY, \
    ANNOTATION_TABLE_KEY, \
    BED3_FILE_KEY, \
    BED6_FILE_KEY, \
    NARROWPEAK_FILE_KEY, \
    WIG_FILE_KEY, \
    BIGWIG_FILE_KEY, \
    BEDGRAPH_FILE_KEY, \
    JSON_FILE_KEY, \
    GENERAL_FILE_KEY, \
    WILDCARD, \
    OBSERVATION_SET_KEY, \
    FEATURE_SET_KEY, \
    PARENT_OP_KEY, \
    RESOURCE_KEY

from .base import DataResource

from .sequence_types import FastAResource, \
    FastQResource, \
    AlignedSequenceResource, \
    BAMIndexResource

from .table_types import Matrix, \
    IntegerMatrix, \
    Network, \
    AnnotationTable, \
    FeatureTable, \
    BED3File, \
    BED6File, \
    NarrowPeakFile

from .general_types import GeneralResource
from .json_types import JsonResource
from .genomic_display_types import WigFileResource, \
    BigWigFileResource, \
    BedGraphFileResource

logger = logging.getLogger(__name__)

# A mapping of the database strings to the classes
# needed to implement the validation.
RESOURCE_MAPPING = {
    FASTQ_KEY: FastQResource, 
    FASTA_KEY: FastAResource,
    ALIGNMENTS_KEY: AlignedSequenceResource,
    BAI_KEY: BAMIndexResource,
    FEATURE_TABLE_KEY: FeatureTable,
    MATRIX_KEY: Matrix,
    INTEGER_MATRIX_KEY: IntegerMatrix,
    EXPRESSION_MATRIX_KEY: Matrix,
    RNASEQ_COUNT_MATRIX_KEY: IntegerMatrix,
    NETWORK_DESCRIPTOR_KEY: Network,
    ANNOTATION_TABLE_KEY: AnnotationTable,
    BED3_FILE_KEY: BED3File,
    BED6_FILE_KEY: BED6File,
    NARROWPEAK_FILE_KEY: NarrowPeakFile,
    WIG_FILE_KEY: WigFileResource,
    BIGWIG_FILE_KEY: BigWigFileResource,
    BEDGRAPH_FILE_KEY: BedGraphFileResource,
    JSON_FILE_KEY: JsonResource,
    WILDCARD: GeneralResource
} 

# These types are skipped when files are validated.
# Files are inferred to be of these types based on 
# their file formats.  Each resource type has a set
# of canonical formats as a class member.  We check
# against those types when determining whether to skip
# validation. For example, if a file ends with "fastq.gz"
# we infer that it is a FastQResource as "fastq.gz" is one
# of the canonical formats of the FastQResource class.
RESOURCE_TYPES_WITHOUT_VALIDATION = set([
    FastAResource,
    FastQResource,
    AlignedSequenceResource,
    BAMIndexResource,
    WigFileResource,
    BigWigFileResource,
    BedGraphFileResource,
    GeneralResource
])

# At minimum, those types that don't have validation methods also 
# will not return resource previews. For instance, we do not want to 
# produce the contents of a FASTQ file. 
RESOURCE_TYPES_WITHOUT_CONTENTS_VIEW = RESOURCE_TYPES_WITHOUT_VALIDATION


def get_resource_type_instance(resource_type_str):
    '''
    This function takes a shorthand identifier (e.g. "MTX") as an
    argument and returns an instantiated instance of the resource 
    type corresponding to that identifier.

    That resource type class provides methods that allow validation, 
    viewing of the resource contents, etc.
    '''
    try:
        resource_type_class = RESOURCE_MAPPING[resource_type_str]
        return resource_type_class()
    except KeyError as ex:
        logger.error('Received an unknown resource_type identifier:'
            ' {resource_type}.  Current types are:'
            ' {resource_mapping}'.format(
                resource_mapping = ','.join(RESOURCE_MAPPING.keys()),
                resource_type = resource_type_str
            )
        )
        raise ex

def get_standard_format(resource_type_str):
    '''
    Returns the "standard" format given a resource type identifier
    '''
    rtc = get_resource_type_instance(resource_type_str)
    return rtc.STANDARD_FORMAT


def get_resource_paginator(resource_type_str):
    '''
    Returns a subclass of the django.core.paginator.Paginator class which 
    will respect their API so that all Resource types can be paginated in 
    a consistent manner
    '''
    # The resource type is the shorthand identifier.
    # To get the actual resource class implementation, we 
    # use the RESOURCE_MAPPING dict
    try:
        resource_class = RESOURCE_MAPPING[resource_type_str]
    except KeyError as ex:
        logger.error('Received a Resource that had a non-null resource_type'
            ' but was also not in the known resource types.'
        )
        return {'error': 'No contents available'}
        
    # instantiate the proper class for this type:
    resource_type = resource_class()
    return resource_type.get_paginator()

def resource_supports_pagination(resource_type_str):
    try:
        get_resource_paginator(resource_type_str)
        logger.info('Was able to successfully obtain a paginator')
        return True
    except NotImplementedError:
        logger.info('Failed to find a paginator class. Default to ignore'
            ' any requests for pagination.'
        )
        return False

def get_acceptable_formats(resource_type):
    '''
    Given the resource type "string", return a list of
    acceptable file formats for that type.
    '''
    resource_class = RESOURCE_MAPPING[resource_type]
    return resource_class.ACCEPTABLE_FORMATS

def format_is_acceptable_for_type(file_format, resource_type):
    '''
    Checks that the file format (as denoted by a string) is consistent with the 
    resource type.  Matching is case-insensitive

    `file_format` is a string which denotes a resource's format,
    e.g. "tsv" for tab-delimited format.
    `resource_type` is one of the keys in RESOURCE_MAPPING.  It 
    is assumed to have already been checked (the serializer from the 
    request validated it already)
    '''
    try:
        acceptable_formats = get_acceptable_formats(resource_type)
    except KeyError as ex:
        logger.info('Received an unacceptable resource type {t}'
            ' when checking file format.'.format(t=ex)
        )
        raise ex
    for ext in acceptable_formats:
        if ext == WILDCARD:
            return True
        if file_format.lower() == ext.lower():
            return True
    return False
