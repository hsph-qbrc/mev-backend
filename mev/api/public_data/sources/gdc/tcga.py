import json
import logging
import datetime
import shutil
import os
import tarfile
from urllib.parse import urlencode

import pandas as pd

from django.conf import settings

from api.utilities.basic_utils import get_with_retry, \
    make_local_directory
from .gdc import GDCDataSource, \
    GDCRnaSeqDataSourceMixin, \
    GDCMethylationDataSourceMixin, \
    GDCMethylationAggregationMixin

logger = logging.getLogger(__name__)


class TCGADataSource(GDCDataSource):
    '''
    A general class for pulling data from TCGA, exposed via the GDC API
    '''

    # All the TCGA-based data will be stored in this directory
    ROOT_DIR = os.path.join(settings.PUBLIC_DATA_DIR, 'tcga')

    def __init__(self):
        if not os.path.exists(self.ROOT_DIR):
            logger.info('When instantiating an instance of TCGADataSource, the'
                ' expected directory did not exist. Go create it...'
            )
            make_local_directory(self.ROOT_DIR)

    def download_and_prep_dataset(self):
        pass

    def get_additional_metadata(self):
        '''
        For the TCGA datasets, we would like an additional mapping from the shorthand ID 
        (e.g. TCGA-LUAD) to the "full" name (e.g. lung adenocarcinoma) 
        '''
        mapping = self.query_for_project_names_within_program('TCGA')
        return {'tcga_type_to_name_map': mapping}


class TCGARnaSeqDataSource(TCGADataSource, GDCRnaSeqDataSourceMixin):
    '''
    A specific implementation of the TCGA data source specific to
    RNA-seq.
    '''

    # A short name (string) which can be used as a "title" for the dataset
    PUBLIC_NAME = 'TCGA RNA-Seq'

    # A longer, more descriptive text explaining the datasource:
    DESCRIPTION = ('TCGA RNA-Seq expression data as processed by the'
        ' Genomic Data Commons'
        ' <a href="https://docs.gdc.cancer.gov/Data/Bioinformatics_Pipelines/Expression_mRNA_Pipeline/">'
        ' mRNA analysis pipeline</a>. Quantifications from this pipeline'
        ' are produced by the STAR aligner.'
    )

    # a string which will make it obvious where the data has come from. For example, we can use
    # this tag to name an output file produced by this class (e.g. the count matrix).
    # We also use this tag
    TAG = 'tcga-rnaseq'

    # An example of how one might query this dataset, so we can provide useful
    # help for dataset creation errors:
    EXAMPLE_PAYLOAD = {
        'TCGA-UVM': ["<UUID>","<UUID>"],
        'TCGA-MESO': ["<UUID>","<UUID>", "<UUID>"]
    }

    def __init__(self):
        super().__init__()
        self.date_str = datetime.datetime.now().strftime('%m%d%Y')

    def prepare(self):
        '''
        Entry method for downloading and munging the TCGA RNA-seq dataset
        to a HDF5 file
        '''
        self._pull_data('TCGA', self.TAG)

    def create_from_query(self, dataset_db_instance, query_filter, output_name = ''):
        return GDCRnaSeqDataSourceMixin.create_from_query(
            self, dataset_db_instance, query_filter, output_name
        )

    def verify_files(self, file_dict):
        return GDCRnaSeqDataSourceMixin.verify_files(self, file_dict)

    def get_indexable_files(self, file_dict):
        return GDCRnaSeqDataSourceMixin.get_indexable_files(self, file_dict)

    def get_additional_metadata(self):
        '''
        This just uses the parent method which maps the TCGA IDs to
        the name (e.g. TCGA-LUAD --> Lung adenocarcinoma)
        '''
        # uses the get_additional_metadata method of TCGADataSource
        # per python's MRO
        return super().get_additional_metadata()


class TCGAMicroRnaSeqDataSource(TCGADataSource, GDCRnaSeqDataSourceMixin):
    '''
    A specific implementation of the TCGA data source specific to
    Micro RNA-seq.

    Note that we use most of the same functionality as the bulk RNA-seq
    and override methods where necessary for our purposes.
    '''

    # A short name (string) which can be used as a "title" for the dataset
    PUBLIC_NAME = 'TCGA microRNA-Seq'

    # A longer, more descriptive text explaining the datasource:
    DESCRIPTION = ('TCGA microRNA-Seq expression data as processed by the'
        ' Genomic Data Commons'
        ' <a href="https://docs.gdc.cancer.gov/Data/Bioinformatics_Pipelines/miRNA_Pipeline/">'
        ' microRNA analysis pipeline</a>. Quantifications from this pipeline'
        ' are refer to read counts summed to known miRNA species in the miRBase reference.'
    )

    # a string which will make it obvious where the data has come from. For example, we can use
    # this tag to name an output file produced by this class (e.g. the count matrix).
    # We also use this tag
    TAG = 'tcga-micrornaseq'

    # An example of how one might query this dataset, so we can provide useful
    # help for dataset creation errors:
    EXAMPLE_PAYLOAD = {
        'TCGA-UVM': ["<UUID>","<UUID>"],
        'TCGA-MESO': ["<UUID>","<UUID>", "<UUID>"]
    }

    # This list defines further filters which are specific to this class where we
    # are getting data regarding STAR-based RNA-seq counts. This list is in addition
    # to any other FILTER_LIST class attributes defined in parent classes. We will
    # ultimately combine them using a logical AND to create the final filter for our query
    # to the GDC API.
    MICRO_RNASEQ_FILTERS = [
        {
            "op": "in",
            "content":{
                "field": "files.analysis.workflow_type",
                "value": ["BCGSC miRNA Profiling"]
                }
        },
        {
            "op": "in",
            "content":{
                "field": "files.experimental_strategy",
                "value": ["miRNA-Seq"]
                }
        }
    ]

    # These are the known values for the 'category' field when we query
    # annotation metadata from the GDC API. 
    # If the dataset is updated to include additional ones, add here. Otherwise
    # the process fails (and reports the new category)
    KNOWN_QC_CATEGORIES = ['Item flagged DNU', 
        'Center QC failed', 
        'Item Flagged Low Quality'
    ]

    # Additional annotation categories that are not used. We ignore these
    IGNORED_CATEGORIES = ['General', 
        'Item is noncanonical',
        'BCR Notification',
        'Barcode incorrect'
    ]

    def __init__(self):
        super().__init__()
        self.date_str = datetime.datetime.now().strftime('%m%d%Y')

    def prepare(self):
        '''
        Entry method for downloading and munging the TCGA RNA-seq dataset
        to a HDF5 file
        '''
        self._pull_data('TCGA', self.TAG)

    def _create_rnaseq_query_params(self, project_id):
        '''
        Internal method to create the GDC-compatible parameter syntax.

        The parameter payload will dictate which data to get, which filters
        to apply, etc.

        Returns a dict

        Note that this overrides the method in
        api.public_data.sources.gdc.gdc.GDCRnaSeqDataSourceMixin
        since that class relies on bulk-RNAseq query params
        '''
        final_filter_list = []

        # a filter for this specific project (e.g. TCGA-LUAD)
        final_filter_list.append(
            GDCDataSource.create_project_specific_filter(project_id))

        # and for the specific RNA-seq data
        final_filter_list.extend(self.MICRO_RNASEQ_FILTERS)

        final_filter = {
            'op': 'and',
            'content': final_filter_list
        }

        basic_fields = GDCDataSource.CASE_FIELDS
        expanded_fields = ','.join(GDCDataSource.CASE_EXPANDABLE_FIELDS)
        final_query_params = GDCDataSource.create_query_params(
            basic_fields,
            expand=expanded_fields,
            filters=json.dumps(final_filter)
        )
        return final_query_params

    def _merge_downloaded_archives(self,
                                   downloaded_archives,
                                   file_to_aliquot_mapping):
        '''
        Given a list of the downloaded archives, extract and
        merge them into a single count matrix
        '''

        logger.info('Begin merging the individual count'
            ' matrix archives into a single count matrix')
        count_df = pd.DataFrame()
        tmpdir = os.path.join(self.ROOT_DIR, 'tmparchive')
        for f in downloaded_archives:
            with tarfile.open(f, 'r:gz') as tf:
                tf.extractall(path=tmpdir)
                for t in tf.getmembers():
                    if t.isfile():
                        if t.name.endswith(
                            'mirbase21.mirnas.quantification.txt'):
                            # the folder has the name of the file.
                            # The prefix UUID on the basename is
                            # not useful to us.
                            file_id = t.name.split('/')[0]
                            df = pd.read_table(
                                os.path.join(tmpdir, t.path),
                                index_col=0,
                                sep='\t',
                                skiprows=1,
                                usecols=[0, 1],
                                names=[
                                    'mirna_id',
                                    file_to_aliquot_mapping[file_id]
                                ]
                            )
                            count_df = pd.concat([count_df, df], axis=1)
                        elif t.name.endswith(
                            'mirbase21.isoforms.quantification.txt'):
                            # the miRNA filters also return file hits
                            # like above. There does not appear to be
                            # a way to filter those out using the
                            # GDC API so we just ignore them here
                            pass
                        elif t.name == 'MANIFEST.txt':
                            pass
                        else:
                            raise Exception(f'Found an unexpected file ({t.name}) '
                                'that did not match our expectations.')
        # Clean up:
        shutil.rmtree(tmpdir)
        return count_df

    def create_from_query(self, dataset_db_instance, query_filter, output_name=''):
        return GDCRnaSeqDataSourceMixin.create_from_query(
            self, dataset_db_instance, query_filter, output_name
        )

    def verify_files(self, file_dict):
        return GDCRnaSeqDataSourceMixin.verify_files(self, file_dict)

    def get_indexable_files(self, file_dict):
        return GDCRnaSeqDataSourceMixin.get_indexable_files(self, file_dict)

    def get_additional_metadata(self):
        '''
        This just uses the parent method which maps the TCGA IDs to
        the name (e.g. TCGA-LUAD --> Lung adenocarcinoma)
        '''
        # uses the get_additional_metadata method of TCGADataSource
        # per python's MRO
        return super().get_additional_metadata()

    def _append_gdc_annotations(self, ann_df):
        '''
        This is an implementation of the stubbed base method
        specific for this micro RNA-seq implementation

        The patient metadata doesn't contain info about library/QC
        issues that can be problematic for miRNA-seq protocols.

        Given an annotation dataframe, use the aliquot IDs to query
        the GDC annotations endpoint to get information about
        potential QC issues.

        As of writing, the annotations of interest are:
        - category: This key uses a finite set of terms
                    to describe issues with the aliquot
        '''

        # Add default 'no' for these. If there are no annotations for
        # a given aliquot, then we assume it's fine
        for c in self.KNOWN_QC_CATEGORIES:
            # solr likes names without spaces, etc. so we replace
            # the GDC-assigned name
            ann_df[c.replace(' ', '_')] = 'N'

        # we can't make a request for the entire annotation 
        # dataframe if it's large. This limits how many we
        # fetch at once
        chunk_size = 100
        N = ann_df.shape[0] // chunk_size
        d = ann_df.shape[0] % chunk_size
        if d > 0:
            N += 1

        entities = ann_df.index.tolist()

        # loop over the chunks of entities (aliquot IDs)
        for i in range(N):
            entity_idx_start = i * chunk_size
            entity_idx_end = (i+1) * chunk_size

            # for each 'chunk' of entities we are searching, we have
            # to potentially loop over paginated results
            finished = False
            pagination_idx = 0
            while not finished:
                start_index = pagination_idx * GDCDataSource.PAGE_SIZE
                end_index = (pagination_idx + 1) * GDCDataSource.PAGE_SIZE
                params={
                    "filters":
                    {
                        "content":[
                            {
                                "content":{
                                    "field":"annotations.entity_id",
                                    "value":entities[entity_idx_start:entity_idx_end]
                                },
                                "op":"in"
                            }
                        ],
                        "op":"and"
                    },
                    "size": GDCDataSource.PAGE_SIZE,
                    "from": start_index
                }

                urlparams = urlencode(params).replace('%27', '%22')
                url = f'{self.GDC_ANNOTATIONS_ENDPOINT}?{urlparams}'
                response = get_with_retry(url)
                response_json = response.json()

                if pagination_idx == 0:
                    pagination_response = response_json['data']['pagination']
                    total_records = int(pagination_response['total'])

                for hit in response_json['data']['hits']:
                    entity_id = hit['entity_id']
                    category = hit['category']
                    if category in self.KNOWN_QC_CATEGORIES:
                        # note the replacement to match the columns dictated above
                        ann_df.loc[entity_id, category.replace(' ', '_')] = 'Y'
                    elif category in self.IGNORED_CATEGORIES:
                        logger.info(f'Ignoring category {category} for entity {entity_id}')
                    else:
                        logger.info('New miRNA-seq annotation category found. Check'
                            f' out the result: {hit}'
                        )
                        raise Exception(f'For entity {entity_id}, found'
                                  f' a new miRNA QC category: {category}')
                pagination_idx += 1
                if end_index >= total_records:
                    finished = True
        return ann_df


class TCGAMethylationDataSource(TCGADataSource, GDCMethylationDataSourceMixin):
    '''
    A specific implementation of the TCGA data source specific to
    methylation data.
    '''

    # A short name (string) which can be used as a "title" for the dataset
    PUBLIC_NAME = 'TCGA Methylation'

    # A longer, more descriptive text explaining the datasource:
    DESCRIPTION = ('TCGA methylation data as processed by the'
        ' Genomic Data Commons'
        ' <a href="https://docs.gdc.cancer.gov/Data/Bioinformatics_Pipelines/Methylation_Pipeline/">'
        ' methylation array harmonization workflow</a>. Methylation "beta" values for CpG sites are'
        ' produced by SeSAMe software.'
    )

    # a string which will make it obvious where the data has come from. For example, we can use
    # this tag to name an output file produced by this class (e.g. the matrix of beta values).
    # We also use this tag
    TAG = 'tcga-methylation'

    # An example of how one might query this dataset, so we can provide useful
    # help for dataset creation errors:
    EXAMPLE_PAYLOAD = {
        'TCGA-UVM': ["<UUID>","<UUID>"],
        'TCGA-MESO': ["<UUID>","<UUID>", "<UUID>"]
    }

    def __init__(self):
        super().__init__()
        self.date_str = datetime.datetime.now().strftime('%m%d%Y')

    def prepare(self):
        '''
        Entry method for downloading and munging the TCGA methylation dataset
        to a HDF5 file
        '''
        self._pull_data('TCGA', self.TAG)

    def create_from_query(self, dataset_db_instance, query_filter, output_name = ''):
        return GDCMethylationDataSourceMixin.create_from_query(
            self, dataset_db_instance, query_filter, output_name
        )

    def verify_files(self, file_dict):
        return GDCMethylationDataSourceMixin.verify_files(self, file_dict)

    def get_indexable_files(self, file_dict):
        return GDCMethylationDataSourceMixin.get_indexable_files(self, file_dict)

    def get_additional_metadata(self):
        '''
        This just uses the parent method which maps the TCGA IDs to
        the name (e.g. TCGA-LUAD --> Lung adenocarcinoma)
        '''
        # uses the get_additional_metadata method of TCGADataSource
        # per python's MRO
        return super().get_additional_metadata()



class TCGAPromoterMethylationDataSource(TCGAMethylationDataSource, GDCMethylationAggregationMixin):
    '''
    A specific implementation of the TCGA data source specific to
    methylation data. This class performs an aggregation of probe-level
    beta values into gene-level values.
    '''

    # A short name (string) which can be used as a "title" for the dataset
    PUBLIC_NAME = 'TCGA Methylation (Gene promoters)'

    # A longer, more descriptive text explaining the datasource:
    DESCRIPTION = ('TCGA methylation data as processed by the'
        ' Genomic Data Commons'
        ' <a href="https://docs.gdc.cancer.gov/Data/Bioinformatics_Pipelines/Methylation_Pipeline/">'
        ' methylation array harmonization workflow</a>. Methylation "beta" values for CpG sites are'
        ' produced by SeSAMe software. Following this, gene-level methylation values are found'
        ' by calculating the mean values for all probes located 200bp upstream of the TSS'
        ' through the first exon of each gene.'
    )

    # a string which will make it obvious where the data has come from. For example, we can use
    # this tag to name an output file produced by this class (e.g. the matrix of beta values).
    # We also use this tag
    TAG = 'tcga-promoter-methylation'

    # An example of how one might query this dataset, so we can provide useful
    # help for dataset creation errors:
    EXAMPLE_PAYLOAD = {
        'TCGA-UVM': ["<UUID>","<UUID>"],
        'TCGA-MESO': ["<UUID>","<UUID>", "<UUID>"]
    }

    # a list of regions that define those we will aggregate over.
    # Since this class concerns promoter methylation, we include
    # regions close to the TSS. Note that the region annotations 
    # are dependent on those in the probe-to-gene mapping file
    TARGET_REGIONS = [
        GDCMethylationDataSourceMixin.TSS_200,
        GDCMethylationDataSourceMixin.FINAL_5PRIME_UTR,
        GDCMethylationDataSourceMixin.FIRST_EXON
    ]

    def _post_process_betas(self, betas_df):
        '''
        Probe-level methylation matrices are exceptionally large and challenging to manage
        within the confines of the WebMeV environment. This method performs aggregation to 
        the gene level to reduce the size of the methylation matrices.

        This method aggregates over `TARGET_REGIONS` which loosely define a promoter-like
        region for each gene. Multiple probes are merged by the mean.
        '''
        return self._aggregate_probes(betas_df, self.TARGET_REGIONS)


class TCGABodyMethylationDataSource(TCGAMethylationDataSource, GDCMethylationAggregationMixin):
    '''
    A specific implementation of the TCGA data source specific to
    methylation data. This class performs an aggregation of gene body-level
    beta values into gene-level values.
    '''

    # A short name (string) which can be used as a "title" for the dataset
    PUBLIC_NAME = 'TCGA Methylation (Gene body)'

    # A longer, more descriptive text explaining the datasource:
    DESCRIPTION = ('TCGA methylation data as processed by the'
        ' Genomic Data Commons'
        ' <a href="https://docs.gdc.cancer.gov/Data/Bioinformatics_Pipelines/Methylation_Pipeline/">'
        ' methylation array harmonization workflow</a>. Methylation "beta" values for CpG sites are'
        ' produced by SeSAMe software. Following this, gene-level methylation values are found'
        ' by calculating the mean values for all probes located downstream of the first exon.'
    )

    # a string which will make it obvious where the data has come from. For example, we can use
    # this tag to name an output file produced by this class (e.g. the matrix of beta values).
    # We also use this tag
    TAG = 'tcga-body-methylation'

    # An example of how one might query this dataset, so we can provide useful
    # help for dataset creation errors:
    EXAMPLE_PAYLOAD = {
        'TCGA-UVM': ["<UUID>","<UUID>"],
        'TCGA-MESO': ["<UUID>","<UUID>", "<UUID>"]
    }

    # a list of regions that define those we will aggregate over.
    # Since this class concerns promoter methylation, we include
    # regions close to the TSS. Note that the region annotations 
    # are dependent on those in the probe-to-gene mapping file
    TARGET_REGIONS = [
        GDCMethylationDataSourceMixin.GENE_BODY,
    ]

    def _post_process_betas(self, betas_df):
        '''
        Probe-level methylation matrices are exceptionally large and challenging to manage
        within the confines of the WebMeV environment. This method performs aggregation to 
        the gene level to reduce the size of the methylation matrices.

        This method aggregates over `TARGET_REGIONS` which loosely define a promoter-like
        region for each gene. Multiple probes are merged by the mean.
        '''
        return self._aggregate_probes(betas_df, self.TARGET_REGIONS)