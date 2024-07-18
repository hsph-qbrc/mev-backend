# This module defines constants that may be required to be 
# accessed across several apps

WILDCARD = '*'

# The following are 'shorthand' IDs for identifying the
# file/resource types in the database. They are also used to map
# particular resource types to their validation logic
# in the `resource_types` package.
FASTQ_KEY = 'FQ'
FASTA_KEY = 'FA'
ALIGNMENTS_KEY = 'ALN'
BAI_KEY = 'BAI'
FEATURE_TABLE_KEY = 'FT'
MATRIX_KEY = 'MTX'
INTEGER_MATRIX_KEY = 'I_MTX'
EXPRESSION_MATRIX_KEY = 'EXP_MTX'
RNASEQ_COUNT_MATRIX_KEY = 'RNASEQ_COUNT_MTX'
NETWORK_DESCRIPTOR_KEY = 'NS'
ANNOTATION_TABLE_KEY = 'ANN'
BED3_FILE_KEY = 'BED3'
BED6_FILE_KEY = 'BED6'
NARROWPEAK_FILE_KEY = 'NRWPK'
WIG_FILE_KEY = 'WIG'
BIGWIG_FILE_KEY = 'BIGWIG'
BEDGRAPH_FILE_KEY = 'BEDGRAPH'
JSON_FILE_KEY = 'JSON'
GENERAL_FILE_KEY = WILDCARD 

# To use with the database, we need a list of tuples connecting
# the shorthand IDs with the human-readable values:
DATABASE_RESOURCE_TYPES = [
    (FASTQ_KEY, 'Fastq'),
    (FASTA_KEY,'Fasta'),
    (ALIGNMENTS_KEY,'Alignment (SAM/BAM)'),
    (BAI_KEY, 'BAM Index'),
    (FEATURE_TABLE_KEY, 'Feature table'),
    (MATRIX_KEY,'Numeric table'),
    (INTEGER_MATRIX_KEY,'Integer table'),
    (EXPRESSION_MATRIX_KEY,'Expression matrix'),
    (RNASEQ_COUNT_MATRIX_KEY,'RNA-seq count matrix'),
    (NETWORK_DESCRIPTOR_KEY, 'Network descriptor'),
    (ANNOTATION_TABLE_KEY,'Annotation table'),
    (BED3_FILE_KEY,'BED3-format file'),
    (BED6_FILE_KEY,'BED6-format file'),
    (NARROWPEAK_FILE_KEY, 'NarrowPeak (BED6+4) format file'),
    (WIG_FILE_KEY, 'WIG (wiggle) format'),
    (BIGWIG_FILE_KEY, 'BigWIG (compressed wiggle) format'),
    (BEDGRAPH_FILE_KEY, 'BedGraph format'),
    (JSON_FILE_KEY,'JSON-format file'),
    (GENERAL_FILE_KEY, 'General file')
]

# For better messages, logging, etc. prepare a map
# based on the list of tuples above.
DB_RESOURCE_KEY_TO_HUMAN_READABLE = {
    x[0]:x[1] for x in DATABASE_RESOURCE_TYPES
}

RESOURCE_TYPE_SET = set(DB_RESOURCE_KEY_TO_HUMAN_READABLE.keys())

# These constants are related to metadata (api.models.ResourceMetadata)
# which can be associated with api.models.Resource instances
OBSERVATION_SET_KEY = 'observation_set'
FEATURE_SET_KEY = 'feature_set'
PARENT_OP_KEY = 'parent_operation'
RESOURCE_KEY = 'resource'

# These are "format" strings which dictate how a file should be parsed
# Generally, these correspond with conventional file extensions
CSV_FORMAT = 'csv'
TSV_FORMAT = 'tsv'
XLS_FORMAT = 'xls'
XLSX_FORMAT = 'xlsx'
JSON_FORMAT = 'json'
FASTA_FORMAT = 'fa'
FASTQ_FORMAT = 'fq'
BAM_FORMAT = 'bam'
WIG_FORMAT = 'wig'
BIGWIG_FORMAT = 'bw'
BEDGRAPH_FORMAT = 'bg'
UNSPECIFIED_FORMAT = WILDCARD

# These are text descriptions for each of the formats. Used to assist
# clients in choosing the appropriate file format
CSV_DESCRIPTION = ('Comma-delimited format. Each field is separated'
    ' by a comma character.'
)
TSV_DESCRIPTION = ('Tab-delimited format. Each field is separated'
    ' by a tab character.'
)
XLS_DESCRIPTION = ('Excel file, typically denoted by the "XLS" file'
    ' extension. Used by Microsoft Excel 97, Microsoft Excel 2000,'
    '  Microsoft Excel 2002, and Microsoft Office Excel 2003.'
)
XLSX_DESCRIPTION = ('Excel file, typically denoted by the "XLSX" file'
    ' extension. This is common for spreadsheets created by recent'
    ' versions of MS Office.'
)
JSON_DESCRIPTION = ('A plain-text file conforming to JavaScript object'
    ' notation, a flexible format for encoding diverse data.'
)
FASTA_DESCRIPTION = ('FASTA-format sequence file, typically'
    ' denoted with a "fa" file extension.'
)
FASTQ_DESCRIPTION = ('FASTQ-format sequence file, typically'
    ' denoted with a "fq" or "fastq" file extension.'
)
BAM_DESCRIPTION = ('Binary alignment file. This a compressed version'
    ' of the standard SAM file format.'
)
GENERAL_DESCRIPTION = ('A file of undetermined or proprietary format.'
    ' This can often be used for proprietary formats'
    ' (e.g. 10x Genomics CellRanger outputs) or for files where the'
    ' resource type strictly defines the format.'
    ' Certain WebMeV tools will require this resource type and will mention'
    ' this in the tool\'s help fields.'
)

WIG_DESCRIPTION = ('A text file conforming to the WIG format. These files are'
    ' often used for display of dense continuous data, such as coverage'
    ' profiles or probability.'
)

BIGWIG_DESCRIPTION = ('A binary and indexed format often used for display'
    ' of dense continuous data, such as coverage profiles or probability'
)

BEDGRAPH_DESCRIPTION = ('A four-column text based format often used for'
    ' display of sparse and irregular data, such as for a ChIP-seq'
    ' experiment.'
)

# Link the shorthand format ID to the description
FORMATS_MAPPING = {
    CSV_FORMAT: CSV_DESCRIPTION,
    TSV_FORMAT: TSV_DESCRIPTION,
    XLS_FORMAT: XLS_DESCRIPTION,
    XLSX_FORMAT: XLSX_DESCRIPTION,
    JSON_FORMAT: JSON_DESCRIPTION,
    FASTA_FORMAT: FASTA_DESCRIPTION,
    FASTQ_FORMAT: FASTQ_DESCRIPTION,
    BAM_FORMAT: BAM_DESCRIPTION,
    WIG_FORMAT: WIG_DESCRIPTION,
    BIGWIG_FORMAT: BIGWIG_DESCRIPTION,
    BEDGRAPH_FORMAT: BEDGRAPH_DESCRIPTION,
    UNSPECIFIED_FORMAT: GENERAL_DESCRIPTION
}

# Use these values as 'markers' for dataframes/tables that have infinite values.
# Since the data needs to be returned as valid JSON and Inf (and other variants)
# are not permitted
POSITIVE_INF_MARKER = '++inf++'
NEGATIVE_INF_MARKER = '--inf--'

# Used for consistent reference to "directions". For instance
# if a correlation is -0.7 and we are sending a payload describing
# a network, we want to show that this is a strongly, but negatively
# correlated edge. Hence, we want to say the edge weight is 0.7 but
# that it's "negative" (so that any vizualations, etc. can color accordingly)
POSITIVE_MARKER = 'POS'
NEGATIVE_MARKER = 'NEG'

# a consistent column header for our first column. This is added
# after we validate the files and write to storage.
FIRST_COLUMN_ID = '__id__'