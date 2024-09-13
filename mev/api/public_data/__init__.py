import datetime
import os
import logging

from django.core.files import File

from api.models import PublicDataset, Resource
from api.async_tasks.async_resource_tasks import validate_resource
from api.utilities.resource_utilities import create_resource
from api.utilities.basic_utils import delete_local_file, \
    read_local_file
from .sources.gdc.tcga import TCGARnaSeqDataSource, \
    TCGAMicroRnaSeqDataSource, \
    TCGAMethylationDataSource, \
    TCGAPromoterMethylationDataSource, \
    TCGABodyMethylationDataSource
from .sources.gdc.target import TargetRnaSeqDataSource
from .sources.gtex_rnaseq import GtexRnaseqDataSource
from .indexers import get_indexer


logger = logging.getLogger(__name__)

# This is a set of the available public datasets.
# While the database maintains metadata about the datasets, it has no
# concept of the implementation of said datasets. Thus, we use this list
# as a check. Even if the database reports that a particular dataset is
# available, the lack of a corresponding implementation will stop any further
# actions like querying, etc.
# Add the classes to this list as necessary:
IMPLEMENTING_CLASSES = [
    TCGARnaSeqDataSource,
    TargetRnaSeqDataSource,
    GtexRnaseqDataSource,
    TCGAMicroRnaSeqDataSource,
    TCGAMethylationDataSource,
    TCGABodyMethylationDataSource,
    TCGAPromoterMethylationDataSource
]

DATASET_MAPPING = {x.TAG:x for x in IMPLEMENTING_CLASSES}

# Note that we explicitly create this list (instead of using the keys method
# on DATASET_MAPPING) so that we can check that all names are unique in a unit
# test. If we just used the keys method on the dict, then we wouldn't see that.
DATASETS = [x.TAG for x in IMPLEMENTING_CLASSES]


def check_if_valid_public_dataset_name(dataset_id):
    '''
    A basic method that will return a boolean depending on whether
    the passed string (which represents a dataset/solr collection)
    corresponds to an actual implementing class.
    '''
    return dataset_id in DATASETS

def get_implementing_class(index_name):
    implementing_class = DATASET_MAPPING[index_name]
    return implementing_class()

def prepare_dataset(dataset_id):
    '''
    This is the main entrypoint for all public datasets to be created or updated

    Calls to this function should have already checked that the requested
    dataset name is "valid" using the check_if_valid_public_dataset_name
    function in this module.

    Note that this only downloads and prepares the dataset. Does NOT index!
    '''

    # Get the class which will do all the work and instantiate it
    dataset = get_implementing_class(dataset_id)
    dataset.prepare()

def index_dataset(dataset_db_instance, file_mapping):
    '''
    Indexes a subset of the files provided as a dict in the second arg.
    Specifics of the indexing are left to the implementing class and
    may affect which changes are committed.

    `file_mapping` is a dict of keys pointing at file paths. Some of these
    files are indexed, while others are just there to verify the integrity
    of the entire dataset.

    for instance, in the TCGA RNA-seq data, we ONLY index the annotation/metadata
    file, but we still require the presence of a count matrix. 
    '''

    # temporarily inactivate the dataset:
    dataset_db_instance.active = False
    dataset_db_instance.save()

    # the unique dataset ID
    index_name = dataset_db_instance.index_name

    # the implementing class for this dataset
    dataset = get_implementing_class(index_name)

    dataset.verify_files(file_mapping)

    files_to_index = dataset.get_indexable_files(file_mapping)

    # get the implementation for the indexing tool and instantiate
    indexer = get_indexer()
    for filepath in files_to_index:
        try:
            indexer.index(index_name, filepath)
        except Exception as ex:
            return

    # see if that dataset has any additional metadata we'd like to track
    additional_metadata = dataset.get_additional_metadata()

    # Once the index process has successfully completed,
    # update the database model and save:
    dataset_db_instance.public_name = dataset.PUBLIC_NAME
    dataset_db_instance.description = dataset.DESCRIPTION
    dataset_db_instance.timestamp = datetime.datetime.today()
    dataset_db_instance.active = True
    dataset_db_instance.file_mapping = file_mapping
    dataset_db_instance.additional_metadata = additional_metadata
    dataset_db_instance.save()

def query_dataset(dataset_id, query_payload):
    '''
    This is the interface for the view functions to query the index
    for data meeting specific filters/criteria

    `dataset_id` is a unique ID string (corresponds to the unique `index_name`
    field of the database table containing public dataset metadata)

    `query_payload` is a dict which contains the query parameters. 

    The indexers should be able to interpret these two args and form
    the proper query, which is dependent on the indexer technology (e.g. solr)
    '''

    # instantiate the indexer we're using
    indexer = get_indexer()

    if check_if_valid_public_dataset_name(dataset_id):
        return indexer.query(dataset_id, query_payload)
    else: 
        raise Exception('The requested public dataset ({d})'
        ' could not be found.'.format(d=dataset_id))

def create_dataset_from_params(dataset_id, user, request_payload, output_name = ''):
    '''
    This will create a Resource based on the request
    payload. Note that if request_payload is None, then we 
    do not apply any filtering 
    '''

    if not check_if_valid_public_dataset_name(dataset_id):
        raise Exception('Dataset identifier was not valid.')

    ds = get_implementing_class(dataset_id)
    dataset_db_instance = PublicDataset.objects.get(index_name = dataset_id)
    if not dataset_db_instance.active:
        #TODO: improve message
        raise Exception('The requested dataset was not active. If this'
            ' does not resolve, please contact an administrator.'
        )
    try:
        path_list, name_list, resource_type_list, file_format_list = ds.create_from_query(
            dataset_db_instance,
            request_payload,
            output_name
        )
    except Exception as ex:
        logger.info('An error occurred when preparing the file based'
             ' on the following query params: {d}.'.format(d=request_payload)
        )
        raise ex

    # create the Resource instances.
    for path, name, resource_type, file_format in \
            zip(path_list, name_list, resource_type_list, file_format_list):
        with read_local_file(path, mode='rb') as fin:
            fh = File(fin, name)
            r = create_resource(
                user,
                file_handle=fh,
                name=name,
                resource_type=resource_type,
                file_format=file_format,
                status=Resource.VALIDATING
            )

        # although we have full control over the creation of files here,
        # running it through this function ensures that it is properly
        # validated and that the proper metadata is extraced.
        # Previously, the workspace was not populating metadata since
        # it was bypassing this call
        validate_resource.delay(
            r.pk, 
            resource_type,
            file_format
        )

        # if the file validated and a resource was created, then it's safe
        # in the permanent storage. Delete the tmp file:
        delete_local_file(path)