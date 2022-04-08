import json
import re
import logging
import shutil
import os
import requests
import tarfile
import uuid

import pandas as pd

from django.conf import settings

from api.utilities.basic_utils import get_with_retry
from api.public_data.sources.base import PublicDataSource
from api.public_data.sources.rnaseq import RnaSeqMixin

logger = logging.getLogger(__name__)

class GDCDataSource(PublicDataSource):
    '''
    This class handles the logic/behavior for GDC-based data
    sources like TCGA, TARGET, etc.

    Note that in the GDC nomenclature, "program" refers to TCGA, 
    TARGET, etc. and "project" refers to items within that program
    such as TCGA-LUAD, TCGA-BRCA
    '''
    
    GDC_FILES_ENDPOINT = "https://api.gdc.cancer.gov/files"
    GDC_DATA_ENDPOINT = "https://api.gdc.cancer.gov/data"
    GDC_PROJECTS_ENDPOINT = "https://api.gdc.cancer.gov/projects"
    GDC_DICTIONARY_ENDPOINT = 'https://api.gdc.cancer.gov/v0/submission/_dictionary/{attribute}?format=json'

    # How many records to return with each query
    PAGE_SIZE = 100

    # This defines which fields are returned from the data query about cases 
    # These are the top-level fields. Most of the interesting clinical data is 
    # contained in the "expandable" fields which are given below.
    CASE_FIELDS = [
        'file_id',
        'file_name',
        'cases.project.program.name',
        'cases.case_id',
        'cases.aliquot_ids',
        'cases.samples.portions.analytes.aliquots.aliquot_id'
    ]

    # These "expandable" fields have most of the metadata we are interested in.
    # By asking for these in the query, we get back info about race, gender, etc.
    CASE_EXPANDABLE_FIELDS = [
        'cases.demographic',
        'cases.diagnoses',
        'cases.exposures',
        'cases.tissue_source_site',
        'cases.project'
    ]

    @staticmethod
    def create_program_filter(program_id):
        '''
        Returns a GDC-compatible filter (a dict) that allows
        filtering for a specific program (e.g. TCGA, TARGET)

        Note that this assumes you are using the "projects" endpoint
        and not another endpoint like "files"
        '''
        return {
            "op": "in",
            "content":{
                "field": "program.name",
                "value": [program_id]
                }
        }

    @staticmethod
    def query_for_project_names_within_program(program_id):
        '''
        Gets a mapping of the available project names within a 
        GDC program (e.g. all the TCGA cancer types within the TCGA
        program)

        Returns a dict that maps the program ID (e.g. TCGA-LUAD)
        to a "real" name like lung adenocarcinoma

        `program_id` is a string like TCGA or TARGET. One of the GDC
        top-level programs

        '''
        filters = GDCDataSource.create_program_filter(program_id)

        # 'program.name' gives the ID like "TCGA-LUAD" and 
        # 'name' gives a "readable" name like "Lung adenocarcinoma"
        fields = ['program.name', 'name']
        query_params = GDCDataSource.create_query_params(
            fields,
            page_size = 10000, # gets all types at once
            filters = json.dumps(filters)
        )
        r = get_with_retry(
            GDCDataSource.GDC_PROJECTS_ENDPOINT, params=query_params)
        response_json = r.json()
        project_mapping_dict = {x['id']: x['name'] for x in response_json['data']['hits']}
        return project_mapping_dict

    @staticmethod
    def create_project_specific_filter(project_id):
        '''
        Creates/returns a filter for a single project (e.g. TCGA-BRCA)

        This filter (a dict) can be directly added to a filter array
        '''
        return {
            "op": "in",
            "content":{
                "field": "files.cases.project.project_id",
                "value": [project_id]
                }
        }

    def download_and_prep_dataset(self):
        '''
        Used to periodically pull data from the GDC 
        '''
        raise NotImplementedError('You must implement this method in a child class.')

    @staticmethod
    def create_query_params(fields, page_size = None, **kwargs):
        '''
        A GDC-common payload to specify parameters. All parameter payloads are
        structured as such, so we can simply inject the proper fields, etc. 
        as needed. Returns a dict.

        `fields` is a list of strings
        Any kwargs are inserted directly into this dict

        '''
        if page_size is None:
            page_size = GDCDataSource.PAGE_SIZE

        query_params = {
            "fields": ','.join(fields),
            "format": "JSON",
            # cast this int as a string since it becomes a url param:
            "size": str(page_size),
        }
        query_params.update(kwargs)
        return query_params

    @staticmethod
    def merge_with_full_record(full_record, returned_data, aliquot_ids, extra_fields=None):
        '''
        This method is used to merge the actual returned data with a dictionary that has the
        universe of possible data fields. The result is a merged data structure that combines
        the returned data with that full set of potential fields.

        More explanation:

        The GDC "data dictionary" has keys for demographic, diagnosis, etc.
        Each of those keys addresses a list with items like:
        {
            "field": "smokeless_tobacco_quit_age",
            "description": null
        },
        {
            "field": "smoking_frequency",
            "description": "The text term used to generally decribe how often the patient smokes."
        },
        ...

        In the real data, we may not have some of these fields as the metadata is incomplete
        or not a relevant field for a particular case. This function merges the "full" description (all possible 
        fields according to the GDC data dictionary) with the actual data given.

        The `returned_data` field is a list of dicts. For instance, it could look like
        [
        {
            "synchronous_malignancy": "No",
            ...
            "tumor_stage": "stage iva"
        },
        ...
        {
            "synchronous_malignancy": "Yes",
            ...
            "tumor_stage": "stage iva"
        }
        ]

        e.g.:
            >>> mock_data_dict = [
            ...   {'field': 'a', 'description':''},
            ...   {'field': 'c', 'description':''},
            ...   {'field': 'b', 'description':''},
            ...   {'field': 'd', 'description':''},
            ...   {'field': 'e', 'description':''},
            ... ]
            >>> 
            >>> returned_data = [
            ...   {'c':13, 'a':11},
            ...   {},
            ...   {'a':31, 'b':32, 'c':33, 'd':34}
            ... ]
            >>> 
            >>> aliquot_ids = ['a1','a2','a3']
            >>> 
            >>> merge_with_full_record(mock_data_dict, returned_data, aliquot_ids)
                a     c     b     d   e
            a1  11.0  13.0   NaN   NaN NaN
            a2   NaN   NaN   NaN   NaN NaN
            a3  31.0  33.0  32.0  34.0 NaN

        So you can see that even though the first record only contained info about
        'a' and 'c', we end up  with a full row with blanks for the missing fields
        'b', 'd', and 'e'.

        Note that the `extra_fields` kwarg allows us to add more info
        that is NOT in the data dict. For instance, the data dictionary
        for the 'project' attribute (available from 
        https://api.gdc.cancer.gov/v0/submission/_dictionary/{attribute}?format=json) 
        does not define the 'project_id' field. HOWEVER, the payload returned from the file
        query API DOES have this field listed. Thus, to have the project (e.g. TCGA cancer type) 
        as part of the final data, we pass "project_id" within a list via that kwarg.

        Thus,
        full_record: is a list of dicts. Each dict has (at minimum) `name` and `description` keys.
        returned_data: a list of dicts for a set of aliquots. This comes from a paginated query of actual cases
        aliquot_ids: The unique aliquot IDs, which will name the rows (the dataframe index)
        '''
        _fields = [ k['field'] for k in full_record ]

        # Note that the columns kwarg doesn't lose the association of the data and columns.
        # It's NOT like a brute-force column renaming. Instead, it just reorders the columns
        # that already exist
        df = pd.DataFrame(returned_data, index=aliquot_ids, columns=_fields)

        if extra_fields:
            other_df = pd.DataFrame(returned_data, index=aliquot_ids)[extra_fields]
            df = pd.merge(df, other_df, left_index=True, right_index=True)
        return df

    def get_data_dictionary(self):
        '''
        The GDC defines a data schema which we query here. This gives the universe
        of data fields, which are used by children classes.
        '''

        # When querying the data dictionary, we also get extraneous fields
        # we don't care about. Add those to this list:
        IGNORED_PROPERTIES = [
            'cases',
            'state',
            'type',
            'updated_datetime',
            'created_datetime',
            'id',
            'submitter_id',
            'releasable',
            'released',
            'intended_release_date',
            'batch_id',
            'programs'
        ]

        # Rather than getting EVERYTHING, we only query which fields are
        # available within these general categories:
        ATTRIBUTES = [
            'demographic',
            'diagnosis',
            'exposure',
            'project'
        ]

        d = {}
        for attr in ATTRIBUTES:
            property_list = []
            url = self.GDC_DICTIONARY_ENDPOINT.format(attribute = attr)
            response = get_with_retry(url)
            j = response.json()
            properties = j['properties']

            for k in properties.keys():
                if k in IGNORED_PROPERTIES:
                    continue
                try:
                    description = properties[k]['description']
                except KeyError as ex:
                    description = None
                property_list.append({
                    'field': k,
                    'description': description
                })
            d[attr] = property_list
        return d


class GDCRnaSeqDataSourceMixin(RnaSeqMixin):
    '''
    A class that contains methods, filters, etc. that are common to 
    count-based RNA-seq data across the various GDC programs
    '''

    # This list defines further filters which are specific to this class where we
    # are getting data regarding HTSeq-based RNA-seq counts. This list is in addition
    # to any other FILTER_LIST class attributes defined in parent classes. We will
    # ultimately combine them using a logical AND to create the final filter for our query
    # to the GDC API.
    RNASEQ_FILTERS = [
        {
            "op": "in",
            "content":{
                "field": "files.analysis.workflow_type",
                "value": ["HTSeq - Counts"]
                }
        },
        {
            "op": "in",
            "content":{
                "field": "files.experimental_strategy",
                "value": ["RNA-Seq"]
                }
        },
        {
            "op": "in",
            "content":{
                "field": "files.data_type",
                "value": ["Gene Expression Quantification"]
                }
        }
    ]

    # The count files include counts to non-genic features. We don't want those
    # in our final assembled count matrix
    SKIPPED_FEATURES = [
        '__no_feature', 
        '__ambiguous',
        '__too_low_aQual', 
        '__not_aligned', 
        '__alignment_not_unique'
    ]

    # We look for HTSeq-based counts which have this suffix
    HTSEQ_SUFFIX = 'htseq.counts.gz'
    
    # Some of the files are not named consistently and have this file suffix
    HTSEQ_SUFFIX_ALT = 'htseq_counts.txt.gz'


    def verify_files(self, file_dict):
        '''
        A method to verify that all the necessary files are present
        to properly index this dataset.
        '''
        # use the base class to verify that all the necessary files
        # are there
        self.check_file_dict(file_dict)

    def _download_cohort(self, project_id, data_fields):
        '''
        Handles the download of metadata and actual data for a single
        GDC project (e.g. TCGA-LUAD). Will return a tuple of:
        - dataframe giving the metadata (i.e. patient info)
        - count matrix 
        '''
        final_query_params = self._create_rnaseq_query_params(project_id)
        
        # prepare some temporary loop variables
        finished = False
        i = 0
        downloaded_archives = []

        # We have to keep a map of the fileId to the aliquot so we can properly 
        # concatenate the files later
        file_to_aliquot_mapping = {}
        annotation_df = pd.DataFrame()
        while not finished:
            logger.info('Downloading batch %d for %s...' % (i, project_id))

            # the records are paginated, so we have to keep track of which page we are currently requesting
            start_index = i*GDCDataSource.PAGE_SIZE
            end_index = (i+1)*GDCDataSource.PAGE_SIZE
            final_query_params.update(
                {
                    'from': start_index
                }
            )

            try:
                response = get_with_retry(
                    GDCDataSource.GDC_FILES_ENDPOINT, 
                    params = final_query_params
                )
            except Exception as ex:
                logger.info('An exception was raised when querying the GDC for'
                    ' metadata. The exception reads: {ex}'.format(ex=ex)
                )
                return

            if response.status_code == 200:
                response_json = json.loads(response.content.decode("utf-8"))
            else:
                logger.error('The response code was NOT 200, but the request'
                    ' exception was not handled.'
                )
                return

            # If the first request, we can get the total records by examining
            # the pagination data
            if i == 0:
                pagination_response = response_json['data']['pagination']
                total_records = int(pagination_response['total'])

            # now collect the file UUIDs and download
            file_uuid_list = []
            case_id_list = []
            exposures = []
            diagnoses = []
            demographics = []
            projects = []
            aliquot_ids = []

            for hit in response_json['data']['hits']:

                # hit['cases'] is a list. To date, have only seen length of 1, 
                # and it's not clear what a greater length would mean.
                # Hence, catch this and issue an error so we can investigate
                if len(hit['cases']) > 1:
                    logger.info('Encountered an unexpected issue when iterating through the returned hits'
                        ' of a GDC RNA-seq query. We expect the "cases" key for a hit to be of length 1,'
                        ' but this was greater. Returned data was: {k}'.format(k=json.dumps(response_json))
                    )
                    continue

                file_uuid_list.append(hit['file_id'])

                case_item = hit['cases'][0]
                case_id_list.append(case_item['case_id'])

                try:
                    exposures.append(case_item['exposures'][0])
                except KeyError as ex:
                    exposures.append({})

                try:
                    diagnoses.append(case_item['diagnoses'][0])
                except KeyError as ex:
                    diagnoses.append({})

                try:
                    demographics.append(case_item['demographic'])
                except KeyError as ex:
                    demographics.append({})

                try:
                    projects.append(case_item['project'])
                except KeyError as ex:
                    projects.append({})

                try:
                    aliquot_ids.append(case_item['samples'][0]['portions'][0]['analytes'][0]['aliquots'][0]['aliquot_id'])
                except KeyError as ex:
                    # Need an aliquot ID to uniquely identify the column. Fail out
                    logger.error('Encountered an unexpected issue when iterating through the returned hits'
                        ' of a GDC RNA-seq query. We expect that we should be able to drill-down to find a unique aliquot ID.'
                        ' The returned data was: {k}'.format(k=json.dumps(response_json))
                    )
                    return

            logger.info('Adding {n} aliquots'.format(n=len(aliquot_ids)))
            file_to_aliquot_mapping.update(dict(zip(file_uuid_list, aliquot_ids)))

            exposure_df = GDCDataSource.merge_with_full_record(
                data_fields['exposure'], 
                exposures, 
                aliquot_ids
            )

            demographic_df = GDCDataSource.merge_with_full_record(
                data_fields['demographic'], 
                demographics, 
                aliquot_ids
            )

            diagnoses_df = GDCDataSource.merge_with_full_record(
                data_fields['diagnosis'], 
                diagnoses, 
                aliquot_ids
            )

            # note that we keep the extra 'project_id' field in this method call. 
            # That gives us the cancer type such as "TCGA-BRCA", etc.
            project_df = GDCDataSource.merge_with_full_record(
                data_fields['project'], 
                projects, 
                aliquot_ids,
                extra_fields = ['project_id']
            )

            # Remove the extra project_id column from the exposure, demo, and diagnoses dataframes. Otherwise we get duplicated
            # columns that we have to carry around:
            exposure_df = exposure_df.drop('project_id', axis=1)
            diagnoses_df = diagnoses_df.drop('project_id', axis=1)
            demographic_df = demographic_df.drop('project_id', axis=1)

            # Now merge all the dataframes (concatenate horizontally)
            # to get the full metadata/annotations
            ann_df = pd.concat([
                exposure_df,
                demographic_df,
                diagnoses_df,
                project_df
            ], axis=1)

            # Create another series which maps the aliquot IDs to the case ID.
            # That will then be added to the annotation dataframe so we know which 
            # metadata is mapped to each case
            s = pd.Series(dict(zip(aliquot_ids, case_id_list)), name='case_id')

            ann_df = pd.concat([ann_df, s], axis=1)

            # Add to the master dataframe for this cancer type
            annotation_df = pd.concat([annotation_df, ann_df], axis=0)

            # Go get the actual count data for this batch.
            downloaded_archives.append(
                self._download_expression_archives(file_uuid_list)
            )

            i += 1

            # are we done yet???
            if end_index >= total_records:
                finished = True

        logger.info('Completed looping through the batches for {ct}'.format(ct=project_id))

        # Merge and write the count files
        count_df = self._merge_downloaded_archives(downloaded_archives, file_to_aliquot_mapping)
        
        logger.info('For {ct}, created a count matrix with {n} aliquots.'.format(
            ct=project_id, n=count_df.shape[1])
        )

        # Cleanup the downloads
        [os.remove(x) for x in downloaded_archives]

        return annotation_df, count_df

    def _pull_data(self, program_id, tag):
        '''
        Method for downloading and munging an RNA-seq dataset
        to a HDF5 file

        Note that creating a flat file of everything was not performant
        and created a >2Gb matrix. Instead, we organize the RNA-seq data
        hierarchically by splitting into the individual projects (e.g.
        TCGA cancer types).
        Each of those is assigned to a "dataset" in the HDF5 file. Therefore,
        instead of a giant matrix we have to load each time, we can directly
        go to cancer-specific count matrices for much better performance.
        '''

        # first get all the cancer types so we can split the downloads
        # and HDFS file
        project_dict = GDCDataSource.query_for_project_names_within_program(program_id)

        # Get the data dictionary, which will tell us the universe of available
        # fields and how to interpret them:
        data_fields = self.get_data_dictionary()

        total_annotation_df = pd.DataFrame()
        counts_output_path = os.path.join(
            self.ROOT_DIR,
            self.COUNT_OUTPUT_FILE_TEMPLATE.format(tag=tag, date=self.date_str)
        )
        with pd.HDFStore(counts_output_path) as hdf_out:
            for project_id in project_dict.keys():
                logger.info('Pull data for %s' % project_id)
                ann_df, count_df = self._download_cohort(project_id, data_fields)
                total_annotation_df = pd.concat([total_annotation_df, ann_df], axis=0)

                # save the counts to a cancer-specific dataset. Store each
                # dataset in a cancer-specific group. On testing, this seemed
                # to be a bit faster for recall than keeping all the dataframes
                # as datasets in the root group
                group_id = (
                    RnaSeqMixin.create_python_compatible_id(project_id) + '/ds')
                hdf_out.put(group_id, count_df)
                logger.info('Added the {ct} matrix to the HDF5'
                    ' count matrix'.format(ct=project_id)
                )

        # Write all the metadata to a file
        ann_output_path = os.path.join(
            self.ROOT_DIR,
            self.ANNOTATION_OUTPUT_FILE_TEMPLATE.format(tag = tag, date=self.date_str)
        )
        total_annotation_df.to_csv(
            ann_output_path, 
            sep=',', 
            index_label = 'id'
        )
        logger.info('The metadata/annnotation file for your {program} RNA-seq data'
            'is available at {p}'.format(p=ann_output_path, program=program_id))

    def _merge_downloaded_archives(self, downloaded_archives, file_to_aliquot_mapping):
        '''
        Given a list of the downloaded archives, extract and merge them into a single count matrix
        '''
        logger.info('Begin merging the individual count matrix archives into a single count matrix')
        count_df = pd.DataFrame()
        tmpdir = os.path.join(self.ROOT_DIR, 'tmparchive')
        for f in downloaded_archives:
            with tarfile.open(f, 'r:gz') as tf:
                tf.extractall(path=tmpdir)
                for t in tf.getmembers():
                    if t.name.endswith(self.HTSEQ_SUFFIX) or t.name.endswith(self.HTSEQ_SUFFIX_ALT):
                        # the folder has the name of the file.
                        # The prefix UUID on the basename is not useful to us.
                        file_id = t.name.split('/')[0]
                        df = pd.read_table(
                            os.path.join(tmpdir, t.path), 
                            index_col=0, 
                            header=None, 
                            names=['gene', file_to_aliquot_mapping[file_id]])
                        count_df = pd.concat([count_df, df], axis=1)

        # remove the skipped rows which don't correspond to actual gene features
        count_df = count_df.loc[~count_df.index.isin(self.SKIPPED_FEATURES)]

        # The count matrices have Ensembl identifiers like ENSG0000122345.11
        # The 'version' suffix interferes with database lookups (such as for GO terms, etc.)
        # so we strip that off
        count_df.index = count_df.index.map(lambda x: x.split('.')[0])

        # Clean up:
        shutil.rmtree(tmpdir)

        return count_df

    def _create_rnaseq_query_params(self, project_id):
        '''
        Internal method to create the GDC-compatible parameter syntax.

        The parameter payload will dictate which data to get, which filters 
        to apply, etc.

        Returns a dict
        '''
        final_filter_list = []

        # a filter for this specific project (e.g. TCGA-LUAD)
        final_filter_list.append(GDCDataSource.create_project_specific_filter(project_id))

        # and for the specific RNA-seq data
        final_filter_list.extend(self.RNASEQ_FILTERS)

        final_filter = {
            'op': 'and',
            'content': final_filter_list
        }

        basic_fields = GDCDataSource.CASE_FIELDS
        expanded_fields = ','.join(GDCDataSource.CASE_EXPANDABLE_FIELDS)
        final_query_params = GDCDataSource.create_query_params(
            basic_fields,
            expand = expanded_fields,
            filters = json.dumps(final_filter)
        )
        return final_query_params


    def _download_expression_archives(self, file_uuid_list):
        '''
        Given a list of file UUIDs, download those to the local disk.
        Return the path to the downloaded archive.
        '''
        # Download the actual expression data corresponding to the aliquot metadata
        # we've been collecting
        download_params = {"ids": file_uuid_list}
        download_response = requests.post(GDCDataSource.GDC_DATA_ENDPOINT, 
            data = json.dumps(download_params), 
            headers = {"Content-Type": "application/json"}
        )
        response_head_cd = download_response.headers["Content-Disposition"]
        file_name = re.findall("filename=(.+)", response_head_cd)[0]
        fout = os.path.join('/tmp', file_name)
        with open(fout, "wb") as output_file:
            output_file.write(download_response.content)
        return fout