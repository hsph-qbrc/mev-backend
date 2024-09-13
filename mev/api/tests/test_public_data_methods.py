import json
import shutil
import unittest
import unittest.mock as mock
import os
import datetime
import pandas as pd
import numpy as np
import uuid

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from api.tests.base import BaseAPITestCase

from constants import TSV_FORMAT

from api.public_data import DATASETS, \
    index_dataset, \
    create_dataset_from_params
from api.models import PublicDataset, Resource
from api.public_data.sources.base import PublicDataSource
from api.public_data.sources.gdc.gdc import GDCDataSource, \
    GDCRnaSeqDataSourceMixin, \
    GDCMethylationDataSourceMixin, \
    GDCMethylationAggregationMixin
from api.public_data.sources.rnaseq import RnaSeqMixin
from api.public_data.sources.methylation import MethylationMixin
from api.public_data.sources.gdc.tcga import TCGADataSource, \
    TCGAMicroRnaSeqDataSource, \
    TCGAMethylationDataSource, \
    TCGABodyMethylationDataSource, \
    TCGAPromoterMethylationDataSource  
from api.public_data.sources.gdc.target import TargetDataSource
from api.public_data.sources.gtex_rnaseq import GtexRnaseqDataSource
from api.public_data.indexers.solr import SolrIndexer


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_FILES_DIR = os.path.join(THIS_DIR, 'resource_contents_test_files')

class TestSolrIndexer(BaseAPITestCase): 

    def setUp(self):
        '''
        Note that this setup method is implicitly testing the 
        the core creation and indexing methods
        '''
        self.indexer = SolrIndexer()

    @mock.patch('api.public_data.indexers.solr.requests')
    def test_query_call(self, mock_requests):
        '''
        Test the method where we check make a query request
        '''
        query_str = 'facet.field=project_id&q=*:*&rows=2&'
        index_name = 'foo-{s}'.format(s=str(uuid.uuid4()))
        expected_url = '{solr_server}/{idx}/select?{q}'.format(
            q = query_str,
            idx = index_name, 
            solr_server = self.indexer.SOLR_SERVER
        )
        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        d1 = {'a':1, 'b':2}
        mock_response.json.return_value = d1
        mock_requests.get.return_value = mock_response
        mock_reformat_method = mock.MagicMock()
        d2 = {'c':3, 'd':4}
        mock_reformat_method.return_value = d2
        self.indexer._reformat_response = mock_reformat_method
        result = self.indexer.query(index_name, query_str)
        self.assertDictEqual(result, d2)
        mock_requests.get.assert_called_with(expected_url)
        mock_reformat_method.assert_called_with(d1)

    @mock.patch('api.public_data.indexers.solr.requests')
    def test_bad_query_call(self, mock_requests):
        '''
        Test the method where we check make a query request
        but a bad query is supplied. Solr would issue some kind
        of error message which we mock here.
        '''

        query_str = 'foo=bar'
        index_name = 'foo-{s}'.format(s=str(uuid.uuid4()))
        expected_url = '{solr_server}/{idx}/select?{q}'.format(
            q = query_str,
            idx = index_name, 
            solr_server = self.indexer.SOLR_SERVER
        )
        mock_response = mock.MagicMock()
        mock_response.status_code = 400
        d1 = {'error':{'xyz':1, 'msg':'Something bad happened!'}}
        mock_response.json.return_value = d1
        mock_requests.get.return_value = mock_response

        with self.assertRaises(Exception):
            self.indexer.query(index_name, query_str)
        mock_requests.get.assert_called_with(expected_url)


    @mock.patch('api.public_data.indexers.solr.requests')
    def test_core_check_works(self, mock_requests):
        '''
        Test the method where we check if a core exists.
        Here, we mock out the actual request to the solr
        server.    
        '''
        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_requests.get.return_value = mock_response

        self.assertTrue(self.indexer._check_if_core_exists('some name'))

        mock_response.status_code = 400
        mock_requests.get.return_value = mock_response
        self.assertFalse(self.indexer._check_if_core_exists('junk'))

        mock_response.status_code = 404
        mock_requests.get.return_value = mock_response
        self.assertFalse(self.indexer._check_if_core_exists('junk'))

    @mock.patch('api.public_data.indexers.solr.requests.post')
    @mock.patch('api.public_data.indexers.solr.SolrIndexer._check_if_core_exists')
    @mock.patch('api.public_data.indexers.solr.mimetypes')
    def test_index_call_correctly_made(self, mock_mimetypes,
        mock_check_core, 
        mock_post):
        '''
        Tests that we are issuing the proper request to index a file with solr
        '''
        mock_check_core.return_value = True
        mock_content_type = 'text/csv'
        mock_mimetypes.guess_type.return_value = (mock_content_type,None)
        ann_filepath = os.path.join(THIS_DIR, 'public_data_test_files', 'test_annotation_data.csv')
        mock_core_name = 'foo'

        class MockResponse(object):
            def __init__(self):
                self.status_code = 200
            def json(self):
                return 'something'

        mock_response_obj = MockResponse()
        mock_post.return_value = mock_response_obj
        self.indexer.index(mock_core_name, ann_filepath)
        expected_url ='{host}/{core}/update/'.format(
            host=self.indexer.SOLR_SERVER, core=mock_core_name)
        mock_post.assert_called_with(expected_url,
            data=open(ann_filepath, 'r').read(),
            params={'commit': 'true'},
            headers={'content-type': mock_content_type}
        )


class TestPublicDatasets(BaseAPITestCase): 

    fixtures = [settings.TESTING_DB_DUMP]

    def setUp(self):
        self.all_public_datasets = PublicDataset.objects.filter(active=True)

        if len(self.all_public_datasets) == 0:
            raise ImproperlyConfigured('Need at least one active public dataset to'
                ' run this test properly.'
            )

        # grab the first dataset to use in the tests below
        self.test_dataset = self.all_public_datasets[0]

        # need to use an actual file if we are not mocking out the 
        # django.core.files.File class. Doesn't matter what the 
        # contents are
        self.demo_filepath = os.path.join(TEST_FILES_DIR, 'demo_file1.tsv')

    def test_unique_tags(self):
        '''
        This test serves as a check that we did not accidentally duplicate
        a tag (the `TAG` attribute on the implementing public dataset classes)
        '''
        unique_tags = set(DATASETS)
        self.assertTrue(len(unique_tags) == len(DATASETS))

    @mock.patch('api.public_data.get_indexer')
    @mock.patch('api.public_data.get_implementing_class')
    def test_indexing_steps(self, mock_get_implementing_class, mock_get_indexer):
        '''
        This test verifies that the proper methods are called 
        and that the database is updated accordingly
        '''
        index_name = self.test_dataset.index_name

        mock_dataset = mock.MagicMock()
        mock_dataset.PUBLIC_NAME = 'foo'
        mock_dataset.DESCRIPTION = 'desc'
        mock_dataset.get_indexable_files.return_value = ['a','b']
        mock_dataset.get_additional_metadata.return_value = {'something': 100}
        mock_indexer = mock.MagicMock()

        mock_get_implementing_class.return_value = mock_dataset
        mock_get_indexer.return_value = mock_indexer

        file_mapping = {
            'abc': [1,2,3]
        }

        # make the call to the function
        index_dataset(self.test_dataset, file_mapping)

        # verify that the 
        mock_dataset.verify_files.assert_called_with(file_mapping)
        mock_dataset.get_indexable_files.assert_called_with(file_mapping)
        mock_indexer.index.assert_has_calls([
            mock.call(index_name, 'a'),
            mock.call(index_name, 'b')
        ])

        # query the database to check that it was updated
        p = PublicDataset.objects.get(pk=self.test_dataset.pk)
        self.assertDictEqual(p.file_mapping, file_mapping)
        self.assertEqual('foo', p.public_name)
        self.assertEqual('desc', p.description)

    @mock.patch('api.public_data.delete_local_file')
    @mock.patch('api.public_data.validate_resource')
    @mock.patch('api.public_data.get_implementing_class')
    @mock.patch('api.public_data.check_if_valid_public_dataset_name')
    @mock.patch('api.public_data.create_resource')
    @mock.patch('api.public_data.File')
    def test_dataset_creation_steps_case1(self,
            mock_file_class,
            mock_create_resource,
            mock_check_if_valid_public_dataset_name, 
            mock_get_implementing_class,
            mock_validate_resource,
            mock_delete_local_file
        ):
        '''
        Tests the proper methods are called for the process of creating a 
        public dataset for a user. Here, we do not pass an output name
        to the method. The output_name allows a user to give a custom name
        to the exported files. Otherwise they are auto-named
        '''

        dataset_id = self.test_dataset.index_name
        mock_user = mock.MagicMock()
        mock_dataset = mock.MagicMock()
        mock_resource_instance = mock.MagicMock()
        mock_name = 'filename.tsv'
        mock_resource_instance.name = mock_name
        pk=1111
        mock_resource_instance.pk = pk
        resource_type = 'MTX'
        file_format = TSV_FORMAT

        mock_file = mock.MagicMock()
        mock_file_class.return_value = mock_file

        mock_dataset.create_from_query.return_value = (
            [self.demo_filepath], [mock_name], [resource_type], [file_format])

        mock_create_resource.return_value = mock_resource_instance

        mock_check_if_valid_public_dataset_name.return_value = True
        mock_get_implementing_class.return_value = mock_dataset

        # doesn't matter what this actually is since
        # it depends on the actual dataset being implemented.
        request_payload = {
            'filters': []
        }

        create_dataset_from_params(dataset_id, mock_user, request_payload)

        # check the proper methods were called:
        mock_dataset.create_from_query.assert_called_with(self.test_dataset, request_payload, '')
        mock_validate_resource.delay.assert_called_with(mock_resource_instance.pk, resource_type, file_format)
        mock_create_resource.assert_called_with(
            mock_user,
            file_handle=mock_file,
            name = mock_name,
            resource_type=resource_type,
            file_format=file_format,
            status = Resource.VALIDATING
        )

        mock_delete_local_file.assert_called_with(self.demo_filepath)

    @mock.patch('api.public_data.validate_resource')
    @mock.patch('api.public_data.get_implementing_class')
    @mock.patch('api.public_data.check_if_valid_public_dataset_name')
    @mock.patch('api.public_data.create_resource')
    def test_dataset_creation_fails(self, 
        mock_create_resource,
            mock_check_if_valid_public_dataset_name, 
            mock_get_implementing_class,
            mock_validate_resource
        ):
        '''
        Tests that we do not create a Resource in the case where the
        `create_from_query` method fails for some reason
        '''

        dataset_id = self.test_dataset.index_name
        mock_user = mock.MagicMock()
        mock_dataset = mock.MagicMock()

        mock_dataset.create_from_query.side_effect= Exception('something bad!')

        mock_check_if_valid_public_dataset_name.return_value = True
        mock_get_implementing_class.return_value = mock_dataset

        # doesn't matter what this actually is since
        # it depends on the actual dataset being implemented.
        request_payload = {
            'filters': []
        }

        with self.assertRaises(Exception):
            create_dataset_from_params(dataset_id, mock_user, request_payload)

        # check that methods were NOT called:
        mock_create_resource.assert_not_called()
        mock_validate_resource.delay.assert_not_called()


class TestBasePublicDataSource(BaseAPITestCase):

    @mock.patch('api.public_data.sources.base.os')
    def test_check_file_dict(self, mock_os):
        '''
        Tests a method in the base class that asserts the proper
        files were passed during indexing
        '''

        mock_os.path.exists.return_value = True

        pds = PublicDataSource()

        # below, we pass a dict which identifies files passed to the 
        # class. This, for instance, allows us to know which file is the 
        # annotation file(s) and which is the count matrix. Each class
        # implementation has some necessary files and they will define this:
        pds.DATASET_FILES = ['keyA', 'keyB']

        # a valid dict
        fd = {
            'keyA': ['/path/to/fileA.txt'],
            'keyB': ['/path/to/fileB.txt', '/path/to/another_file.txt']
        }
        pds.check_file_dict(fd)

        # has an extra key- ok to ignore
        fd = {
            'keyA': ['/path/to/fileA.txt'],
            'keyB': ['/path/to/fileB.txt', '/path/to/another_file.txt'],
            'keyC': ['']
        }
        pds.check_file_dict(fd)

        # uses the wrong keys- keyA is missing
        fd = {
            'keyC': ['/path/to/fileA.txt'],
            'keyB': ['/path/to/fileB.txt', '/path/to/another_file.txt']
        }
        with self.assertRaisesRegex(Exception, 'keyA'):
            pds.check_file_dict(fd)

        # Each key should address a list
        fd = {
            'keyA': '/path/to/fileA.txt',
            'keyB': ['/path/to/fileB.txt', '/path/to/another_file.txt']
        }
        with self.assertRaisesRegex(Exception, 'keyA'):
            pds.check_file_dict(fd)

        # mock non-existent filepath
        mock_os.path.exists.side_effect = [True, False, True]
        fd = {
            'keyA': ['/path/to/fileA.txt'],
            'keyB': ['/path/to/fileB.txt', '/path/to/another_file.txt']
        }
        with self.assertRaisesRegex(Exception, '/path/to/fileB.txt'):
            pds.check_file_dict(fd)     

        

class TestGDC(BaseAPITestCase): 
    def test_dummy(self):
        self.assertTrue(True)


class TestTCGA(BaseAPITestCase): 
    def test_gets_all_tcga_types(self):
        '''
        Tests that we get the response from querying all the TCGA types
        from the GDC API. While we don't check the full set, we confirm
        that a few known types are there as a sanity check.
        '''
        ds = TCGADataSource()
        tcga_cancer_types = ds.query_for_project_names_within_program('TCGA')
        self.assertTrue('TCGA-BRCA' in tcga_cancer_types.keys())

        self.assertTrue(tcga_cancer_types['TCGA-LUAD'] == 'Lung Adenocarcinoma')

class TestTARGET(BaseAPITestCase): 
    def test_gets_all_target_types(self):
        '''
        Tests that we get the response from querying all the TARGET types
        from the GDC API. While we don't check the full set, we confirm
        that a few known types are there as a sanity check.
        '''
        ds = TargetDataSource()
        target_types = ds.query_for_project_names_within_program('TARGET')
        self.assertTrue('TARGET-NBL' in target_types.keys())

        self.assertTrue(target_types['TARGET-NBL'] == 'Neuroblastoma')


class TestGDCDataSource(BaseAPITestCase):

    def test_apply_additional_filters(self):
        
        mock_ann = pd.DataFrame({
            'a': [1,2,3],
            'b': ['a','b','c'],
            'case_id': ['c1','c2','c3']
        }, index=['i1','i2','i3'])

        X = np.random.randint(0,10, size=(2,3))
        mock_counts = pd.DataFrame(
            X,
            index=['g1', 'g2'],
            columns=['i1', 'i2', 'i3']
        )
        gdc_ds = GDCDataSource()

        # by default, no arg means leave the dataframes as-is
        updated_ann, updated_counts = gdc_ds.apply_additional_filters(
            mock_ann, mock_counts, {})
        self.assertTrue(mock_ann.equals(updated_ann))
        self.assertTrue(mock_counts.equals(updated_counts))

        # when explicitly passing False, also leave as-is
        updated_ann, updated_counts = gdc_ds.apply_additional_filters(
            mock_ann, mock_counts, {'use_case_id':False})
        self.assertTrue(mock_ann.equals(updated_ann))
        self.assertTrue(mock_counts.equals(updated_counts))

        # if True is passed, we change the annotations and matrix
        # to use the case ID as the identifier/row index
        updated_ann, updated_counts = gdc_ds.apply_additional_filters(
            mock_ann, mock_counts, {'use_case_id':True})
        renamed_counts = pd.DataFrame(
            X,
            index=['g1', 'g2'],
            columns=['c1', 'c2', 'c3']
        )
        renamed_ann = pd.DataFrame({
            'a': [1,2,3],
            'b': ['a','b','c']
        }, index=['c1','c2','c3'])
        self.assertTrue(updated_ann.equals(renamed_ann))
        self.assertTrue(updated_counts.equals(renamed_counts))

        # rename using the case ID as above, but this time we have
        # a situation where there are >1 aliquot IDs. Per our requirements
        # (subject to change), we drop any subjects where there were >1 aliquots
        # to avoid unjustified/ambiguous/random logic on which aliquot is selected
        # for a given subject

        # Note the repeated subject/case ID of c1
        mock_ann = pd.DataFrame({
            'a': [1,2,3,4],
            'b': ['a','b','c','d'],
            'case_id': ['c1','c2','c3', 'c1']
        }, index=['i1','i2','i3', 'i4'])
        X = np.random.randint(0,10, size=(2,4))
        mock_counts = pd.DataFrame(
            X,
            index=['g1', 'g2'],
            columns=['i1', 'i2', 'i3', 'i4']
        )
        updated_ann, updated_counts = gdc_ds.apply_additional_filters(
            mock_ann, mock_counts, {'use_case_id':True})
        renamed_counts = pd.DataFrame(
            X[:,1:3],
            index=['g1', 'g2'],
            columns=['c2', 'c3']
        )
        renamed_ann = pd.DataFrame({
            'a': [2,3],
            'b': ['b','c']
        }, index=['c2','c3'])
        self.assertTrue(updated_ann.equals(renamed_ann))
        self.assertTrue(updated_counts.equals(renamed_counts))


class TestGDCRnaSeqMixin(BaseAPITestCase): 

    def test_proper_filters_created(self):
        '''
        Tests that the json payload for a metadata
        query is created as expected
        '''
        ds = GDCRnaSeqDataSourceMixin()
        d = ds._create_rnaseq_query_params('TCGA-FOO')

        # Note that in the dict below, the value of the 'filters' key is itself a JSON
        # format string. The GDC API will not accept if that value happened to be a native
        # python dict
        expected_query_filters = {
            "fields": "file_id,file_name,cases.project.program.name,cases.case_id,cases.aliquot_ids,cases.samples.portions.analytes.aliquots.aliquot_id",
            "format": "JSON",
            "size": "100",
            "expand": "cases.demographic,cases.diagnoses,cases.exposures,cases.tissue_source_site,cases.project",
            "filters": "{\"op\": \"and\", \"content\": [{\"op\": \"in\", \"content\": {\"field\": \"files.cases.project.project_id\", \"value\": [\"TCGA-FOO\"]}}, {\"op\": \"in\", \"content\": {\"field\": \"files.analysis.workflow_type\", \"value\": [\"STAR - Counts\"]}}, {\"op\": \"in\", \"content\": {\"field\": \"files.experimental_strategy\", \"value\": [\"RNA-Seq\"]}}, {\"op\": \"in\", \"content\": {\"field\": \"files.data_type\", \"value\": [\"Gene Expression Quantification\"]}}]}"
        }
        self.assertDictEqual(d, expected_query_filters)      

    @mock.patch('api.public_data.sources.rnaseq.RnaSeqMixin.COUNT_OUTPUT_FILE_TEMPLATE', '__TEST__counts.{tag}.{ds}.tsv')
    def test_counts_merged_correctly(self):
        file_to_aliquot_mapping = {
            's1': 'x1',
            's2': 'x2',
            's3': 'x3'
        }
        expected_matrix = pd.DataFrame(
            [[509, 1446, 2023],[0,2,22],[1768, 2356, 1768]],
            index=['ENSG00000000003','ENSG00000000005','ENSG0000000000419'],
            columns = ['x1', 'x2', 'x3']
        )
        expected_matrix.index.name = 'gene'

        archives = [
            os.path.join(THIS_DIR, 'public_data_test_files', 'archive1.tar.gz'),
            os.path.join(THIS_DIR, 'public_data_test_files', 'archive2.tar.gz')
        ]

        data_src = GDCRnaSeqDataSourceMixin()
        # The derived classes will have a ROOT_DIR attribute, but
        # this mixin class doesn't. Patch it here
        data_src.ROOT_DIR = '/tmp'
        actual_df = data_src._merge_downloaded_archives(archives, file_to_aliquot_mapping)
        self.assertTrue(expected_matrix.equals(actual_df))

class TestGTExRnaseq(BaseAPITestCase):

    @mock.patch('api.public_data.sources.gtex_rnaseq.GtexRnaseqDataSource._get_sample_annotations')
    @mock.patch('api.public_data.sources.gtex_rnaseq.GtexRnaseqDataSource._get_phenotype_data')
    @mock.patch('api.public_data.sources.gtex_rnaseq.GtexRnaseqDataSource._download_file')
    @mock.patch('api.public_data.sources.gtex_rnaseq.GtexRnaseqDataSource._create_tmp_dir')
    @mock.patch('api.public_data.sources.gtex_rnaseq.run_shell_command')
    def test_data_prep(self, \
        mock_run_shell_command, \
        mock_create_tmp_dir, \
        mock_download_file, \
        mock_get_phenotype_data, \
        mock_get_sample_annotations):
        '''
        Test that we munge everything correctly
        '''
        mock_tmp_dir = os.path.join(
            THIS_DIR, 
            'public_data_test_files'
        )
        mock_create_tmp_dir.return_value = mock_tmp_dir
        ann_path = os.path.join(
            THIS_DIR, 
            'public_data_test_files', 
            'gtex_rnaseq_ann.tsv'
        )
        pheno_path = os.path.join(
            THIS_DIR, 
            'public_data_test_files', 
            'gtex_rnaseq_pheno.tsv'
        )

        mock_get_sample_annotations.return_value = pd.read_table(ann_path)
        mock_get_phenotype_data.return_value = pd.read_table(pheno_path)

        tmp_testing_dir = os.path.join(settings.DATA_DIR, 'test-gtex-rnaseq')
        os.mkdir(tmp_testing_dir)
        GtexRnaseqDataSource.ROOT_DIR = tmp_testing_dir 
        mock_adipose_url = 'adipose_url'
        mock_blood_url = 'blood_url'
        GtexRnaseqDataSource.TISSUE_TO_FILE_MAP = {
            'Adipose - Subcutaneous': mock_adipose_url,
            'Whole Blood': mock_blood_url
        }
        data_src = GtexRnaseqDataSource()
        data_src.prepare()
        f = RnaSeqMixin.COUNT_OUTPUT_FILE_TEMPLATE.format(
            tag = data_src.TAG,
            date = data_src.date_str,
            file_format = TSV_FORMAT
        )
        ann_output = RnaSeqMixin.ANNOTATION_OUTPUT_FILE_TEMPLATE.format(
            tag = data_src.TAG,
            date = data_src.date_str,
            file_format = TSV_FORMAT
        )
        expected_output_hdf = os.path.join(tmp_testing_dir, f)
        expected_output_ann = os.path.join(tmp_testing_dir, ann_output)
        self.assertTrue(os.path.exists(expected_output_hdf))
        self.assertTrue(os.path.exists(expected_output_ann))

        expected_tissue_list = [
            'Adipose - Subcutaneous', 
            'Whole Blood'
        ]
        converted_tissue_list = [RnaSeqMixin.create_python_compatible_id(x) for x in expected_tissue_list]
        groups_list = ['/{x}/ds'.format(x=x) for x in converted_tissue_list]
        with pd.HDFStore(expected_output_hdf) as hdf:
            self.assertCountEqual(groups_list, list(hdf.keys()))

        # cleanup the test folder
        shutil.rmtree(tmp_testing_dir)

        shell_calls = []
        for i in range(2):
            f = os.path.join(mock_tmp_dir, 'f{x}.gct.gz'.format(x=i))
            shell_calls.append(mock.call('gunzip {f}'.format(f=f)))
        mock_run_shell_command.assert_has_calls(shell_calls)
        mock_download_file.assert_has_calls([
            mock.call(
                mock_adipose_url, os.path.join(mock_tmp_dir, 'f0.gct.gz')
            ),
            mock.call(
                mock_blood_url, os.path.join(mock_tmp_dir, 'f1.gct.gz'))
        ])


class TestRnaSeqMixin(BaseAPITestCase): 

    def test_indexes_only_annotation_file(self):
        '''
        An RNA-seq dataset consists of a metadata file and a count matrix.
        This verifies that the `get_indexable_files`  method only returns
        the annotation file
        '''

        data_src = RnaSeqMixin()

        fd = {
            RnaSeqMixin.ANNOTATION_FILE_KEY: ['/path/to/A.txt'],
            RnaSeqMixin.COUNTS_FILE_KEY:['/path/to/counts.tsv'] 
        }
        result = data_src.get_indexable_files(fd)
        self.assertCountEqual(result, fd[RnaSeqMixin.ANNOTATION_FILE_KEY])

    @mock.patch('api.public_data.sources.rnaseq.uuid')
    def test_filters_hdf_correctly(self, mock_uuid_mod):
        '''
        Tests that we filter properly for a 
        dummy dataset stored in HDF5 format.
        '''
        hdf_path = os.path.join(
            THIS_DIR, 
            'public_data_test_files', 
            'tcga_rnaseq.hd5'
        )

        ann_path = os.path.join(
            THIS_DIR, 
            'public_data_test_files', 
            'tcga_rnaseq_ann.csv'
        )

        expected_df = pd.DataFrame(
            [[26,86,67],[54,59,29],[24,12,37]],
            index = ['gA', 'gB', 'gC'],
            columns = ['s1','s3','s5']
        )
        ann_df = pd.DataFrame(
            [['TCGA-ABC', 1990],['TCGA-ABC', 1992], ['TCGA-DEF', 1994]],
            index = ['s1','s3','s5'],
            columns = ['cancer_type', 'year_of_birth']
        )

        # create 5 mock UUIDs. The first two are used in the 
        # first call to the tested method. The final 3 are used in the second
        # call to the tested method. The reason for that is we auto-generate
        # the output filename when the calling function has not provided an 
        # `output_name` arg to the method. In the first call to the tested
        # method, we provide that name, so only two calls are made to the 
        # uuid.uuid4 function. In the second call, we omit that arg and we 
        # hence make an extra call to the uuid4 func.
        mock_uuids = [uuid.uuid4() for i in range(5)]
        mock_uuid_mod.uuid4.side_effect = mock_uuids

        # this dict is what the database record is expected to contain
        # in the file_mapping field
        mock_mapping = {
            # this key doesn't matter- we just include it as a correct
            RnaSeqMixin.ANNOTATION_FILE_KEY: [ann_path],
            RnaSeqMixin.COUNTS_FILE_KEY:[hdf_path] 

        }
        mock_db_record = mock.MagicMock()
        mock_db_record.file_mapping = mock_mapping
        query = {
            RnaSeqMixin.SELECTION_KEY: {
                'TCGA-ABC': ['s1', 's3'],
                'TCGA-DEF': ['s5']
            }
        }

        data_src = RnaSeqMixin()

        # the children classes will have a TAG attribute. Since we are
        # testing this mixin here, we simply patch it
        tag = 'foo'
        data_src.TAG = tag
        output_name = 'abc'
        
        # also need to patch the mixin class with a method that would normally
        # be part of the actual dataset class
        mock_apply_additional_filters = mock.MagicMock()
        mock_apply_additional_filters.return_value = (ann_df, expected_df)
        data_src.apply_additional_filters = mock_apply_additional_filters

        paths, filenames, resource_types, file_formats = data_src.create_from_query(mock_db_record, query, output_name)

        mock_apply_additional_filters.assert_called()

        # The order of these doesn't matter in practice, but to check the file contents,
        # we need to be sure we're looking at the correct files for this test.
        self.assertTrue(resource_types[0] == 'RNASEQ_COUNT_MTX')
        self.assertTrue(resource_types[1] == 'ANN')
        self.assertCountEqual(file_formats, [TSV_FORMAT, TSV_FORMAT])

        actual_df = pd.read_table(paths[0], index_col=0)
        self.assertTrue(actual_df.equals(expected_df))

        actual_df = pd.read_table(paths[1], index_col=0)
        self.assertTrue(actual_df.equals(ann_df))

        self.assertEqual(filenames[0], '{x}_counts.{t}.tsv'.format(x=output_name, t=tag))
        self.assertEqual(filenames[1], '{x}_ann.{t}.tsv'.format(x=output_name, t=tag))

        # use index 4 below as 2 uuid.uuid4 calls were 'consumed' by the first call to `create_from_query`
        # while the second call  (the one we are testing now) uses 3 calls to the mock UUID method
        # since there the `output_name` arg was not supplied
        paths, filenames, resource_types, file_formats = data_src.create_from_query(mock_db_record, query)
        self.assertEqual(filenames[0], '{t}_counts.{u}.tsv'.format(u=mock_uuids[4], t=tag))
        self.assertEqual(filenames[1], '{t}_ann.{u}.tsv'.format(u=mock_uuids[4], t=tag))
        self.assertCountEqual(file_formats, [TSV_FORMAT, TSV_FORMAT])


    def test_rejects_whole_dataset_with_null_filter(self):
        '''
        Tests that we reject the request (raise an exception)
        if a filter of None is applied. This would be too large 
        for us to handle.
        '''
        hdf_path = os.path.join(
            THIS_DIR, 
            'public_data_test_files', 
            'tcga_rnaseq.hd5'
        )

        # this dict is what the database record is expected to contain
        # in the file_mapping field
        mock_mapping = {
            # this key doesn't matter- we just include it as a correct
            # representation of the database record
            RnaSeqMixin.ANNOTATION_FILE_KEY: ['/dummy.tsv'],
            RnaSeqMixin.COUNTS_FILE_KEY:[hdf_path] 

        }
        mock_db_record = mock.MagicMock()
        mock_db_record.file_mapping = mock_mapping
        data_src = RnaSeqMixin()
        data_src.PUBLIC_NAME = 'foo' # the actual implementing class would define this attr typically
        with self.assertRaisesRegex(Exception, 'too large'):
            path, resource_type = data_src.create_from_query(mock_db_record, None)

    def test_filters_with_cancer_type(self):
        '''
        Tests that we handle a bad group ID appropriately
        '''
        hdf_path = os.path.join(
            THIS_DIR, 
            'public_data_test_files', 
            'tcga_rnaseq.hd5'
        )

        # this dict is what the database record is expected to contain
        # in the file_mapping field
        mock_mapping = {
            # this key doesn't matter- we just include it as a correct
            # representation of the database record
            RnaSeqMixin.ANNOTATION_FILE_KEY: ['/dummy.tsv'],
            RnaSeqMixin.COUNTS_FILE_KEY:[hdf_path] 

        }
        mock_db_record = mock.MagicMock()
        mock_db_record.file_mapping = mock_mapping
        query = {
            RnaSeqMixin.SELECTION_KEY: {
                # the only datasets in the hdf5 file are for TCGA-ABC
                # and TCGA-DEF. Below, we ask for a non-existant one
                'TCGA-ABC': ['s1', 's3'],
                'TCGA-XYZ': ['s5']
            }
        }
        data_src = RnaSeqMixin()
        with self.assertRaisesRegex(Exception, 'TCGA-XYZ'):
            paths, resource_types = data_src.create_from_query(mock_db_record, query)

    def test_filters_with_bad_sample_id(self):
        '''
        Tests that we handle missing samples appropriately
        '''
        hdf_path = os.path.join(
            THIS_DIR, 
            'public_data_test_files', 
            'tcga_rnaseq.hd5'
        )

        # this dict is what the database record is expected to contain
        # in the file_mapping field
        mock_mapping = {
            # this key doesn't matter- we just include it as a correct
            # representation of the database record
            RnaSeqMixin.ANNOTATION_FILE_KEY: ['/dummy.tsv'],
            RnaSeqMixin.COUNTS_FILE_KEY:[hdf_path] 

        }
        mock_db_record = mock.MagicMock()
        mock_db_record.file_mapping = mock_mapping
        query = {
            RnaSeqMixin.SELECTION_KEY: {
                # add a bad sample ID to the TCGA-ABC set:
                'TCGA-ABC': ['s1111', 's3'],
                'TCGA-DEF': ['s5']
            }
        }
        data_src = RnaSeqMixin()
        with self.assertRaisesRegex(Exception, 's1111'):
            paths, resource_types = data_src.create_from_query(mock_db_record, query)

    def test_empty_filters(self):
        '''
        Tests that we reject if the filtering list is empty
        '''
        hdf_path = os.path.join(
            THIS_DIR, 
            'public_data_test_files', 
            'tcga_rnaseq.hd5'
        )

        # this dict is what the database record is expected to contain
        # in the file_mapping field
        mock_mapping = {
            # this key doesn't matter- we just include it as a correct
            # representation of the database record
            RnaSeqMixin.ANNOTATION_FILE_KEY: ['/dummy.tsv'],
            RnaSeqMixin.COUNTS_FILE_KEY:[hdf_path] 

        }
        mock_db_record = mock.MagicMock()
        mock_db_record.file_mapping = mock_mapping
        query = {
            RnaSeqMixin.SELECTION_KEY:{
                # This should have some strings:
                'TCGA-DEF': []
            }
        }
        data_src = RnaSeqMixin()
        with self.assertRaisesRegex(Exception, 'empty'):
            paths, names, resource_types = data_src.create_from_query(mock_db_record, query)
            #paths, names, resource_types = data_src.create_from_query(query)

    def test_malformatted_filter_dict(self):
        '''
        Tests that we reject if the cancer type refers to something
        that is NOT a list
        '''
        hdf_path = os.path.join(
            THIS_DIR, 
            'public_data_test_files', 
            'tcga_rnaseq.hd5'
        )

        # this dict is what the database record is expected to contain
        # in the file_mapping field
        mock_mapping = {
            # this key doesn't matter- we just include it as a correct
            # representation of the database record
            RnaSeqMixin.ANNOTATION_FILE_KEY: ['/dummy.tsv'],
            RnaSeqMixin.COUNTS_FILE_KEY:[hdf_path] 

        }
        mock_db_record = mock.MagicMock()
        mock_db_record.file_mapping = mock_mapping
        query = {
            RnaSeqMixin.SELECTION_KEY:{
                # This should be a list:
                'TCGA-DEF':'abc'
            }
        }
        data_src = RnaSeqMixin()
        # again, the children will provide an EXAMPLE_PAYLOAD attribute
        # which we patch into this mixin class here
        data_src.EXAMPLE_PAYLOAD = {
        'TCGA-UVM': ["<UUID>","<UUID>"],
        'TCGA-MESO': ["<UUID>","<UUID>", "<UUID>"]
        }
        with self.assertRaisesRegex(Exception, 'a list of sample identifiers'):
            paths, resource_types = data_src.create_from_query(mock_db_record, query)

    def test_missing_selection_key(self):
        '''
        Tests that we properly warn if the format of the request was incorrect.
        Here, we expect that the chosen samples are addressed by 
        RnaSeqMixin.SELECTION_KEY. We leave that out here. So even though
        the data structure itself is fine, the overall payload is malformatted
        '''
        hdf_path = os.path.join(
            THIS_DIR, 
            'public_data_test_files', 
            'tcga_rnaseq.hd5'
        )
        ann_path = os.path.join(
            THIS_DIR, 
            'public_data_test_files', 
            'tcga_rnaseq_ann.csv'
        )

        # this dict is what the database record is expected to contain
        # in the file_mapping field
        mock_mapping = {
            # this key doesn't matter- we just include it as a correct
            # representation of the database record
            RnaSeqMixin.ANNOTATION_FILE_KEY: [ann_path],
            RnaSeqMixin.COUNTS_FILE_KEY:[hdf_path] 

        }
        mock_db_record = mock.MagicMock()
        mock_db_record.file_mapping = mock_mapping
        query = {
            'TCGA-ABC': ['s1', 's3'],
            'TCGA-DEF': ['s5']
        }
        
        data_src = RnaSeqMixin()

        expected_err = 'please pass an object addressed by selections'
        with self.assertRaisesRegex(Exception, expected_err):
            paths, resource_types = data_src.create_from_query(mock_db_record, query)


class TestTCGAMirnaSeq(BaseAPITestCase): 
    '''
    Tests the specialized methods of the class which handles
    TCGA miRNA-seq
    '''

    def test_proper_filters_created(self):
        '''
        Tests that the json payload for a metadata
        query is created as expected
        '''
        ds = TCGAMicroRnaSeqDataSource()
        d = ds._create_rnaseq_query_params('TCGA-FOO')
        expected_query_filters = {
            "fields": "file_id,file_name,cases.project.program.name,cases.case_id,cases.aliquot_ids,cases.samples.portions.analytes.aliquots.aliquot_id",
            "format": "JSON",
            "size": "100",
            "expand": "cases.demographic,cases.diagnoses,cases.exposures,cases.tissue_source_site,cases.project",
            "filters": "{\"op\": \"and\", \"content\": [{\"op\": \"in\", \"content\": {\"field\": \"files.cases.project.project_id\", \"value\": [\"TCGA-FOO\"]}}, {\"op\": \"in\", \"content\": {\"field\": \"files.analysis.workflow_type\", \"value\": [\"BCGSC miRNA Profiling\"]}}, {\"op\": \"in\", \"content\": {\"field\": \"files.experimental_strategy\", \"value\": [\"miRNA-Seq\"]}}]}"
            }
        self.assertDictEqual(d, expected_query_filters)      

    @mock.patch('api.public_data.sources.rnaseq.RnaSeqMixin.COUNT_OUTPUT_FILE_TEMPLATE', '__TEST__counts.{tag}.{date}.hd5')
    def test_counts_merged_correctly(self):
        file_to_aliquot_mapping = {
            's1': 'x1',
            's2': 'x2',
            's3': 'x3'
        }
        expected_matrix = pd.DataFrame(
            [[509, 1446, 2023],[0,2,22],[1768, 2356, 1768]],
            index=['hsa-let-7a','hsa-mir-1-1','hsa-mir-10a'],
            columns = ['x1', 'x2', 'x3']
        )
        expected_matrix.index.name = 'mirna_id'

        archives = [
            os.path.join(THIS_DIR, 'public_data_test_files', 'mirna_archive1.tar.gz'),
            os.path.join(THIS_DIR, 'public_data_test_files', 'mirna_archive2.tar.gz')
        ]

        data_src = TCGAMicroRnaSeqDataSource()
        # The derived classes will have a ROOT_DIR attribute, but
        # this mixin class doesn't. Patch it here
        data_src.ROOT_DIR = '/tmp'
        actual_df = data_src._merge_downloaded_archives(archives, file_to_aliquot_mapping)
        self.assertTrue(expected_matrix.equals(actual_df))

    @mock.patch('api.public_data.sources.gdc.tcga.get_with_retry')
    @mock.patch('api.public_data.sources.gdc.tcga.GDCDataSource')
    def test_annotation_data_addition_case1(self, mock_GDCDataSource, mock_get_with_retry):
        '''
        Test that we properly add 'dummy' values for annotation categories if no 
        annotations are returned
        '''
        data_src = TCGAMicroRnaSeqDataSource()

        # override this attribute so we can mock paginated responses below:
        mock_GDCDataSource.PAGE_SIZE=1

        # create a dummy dataframe to append to
        ann_df = pd.DataFrame({'sex':['M','F','F'], 'age':[35,46,54]}, index=['A', 'B', 'C'])

        # first try an empty response:
        mock_response = mock.MagicMock()
        mock_response_json = {
            'data': {
                'hits': [], 
                'pagination': {
                    'count': 0, 
                    'total': 0, 
                    'size': 100, 
                    'from': 0, 
                    'sort': '', 
                    'page': 0, 
                    'pages': 0
                }
            }, 
            'warnings': {}
        }
        mock_response.json.return_value = mock_response_json
        mock_get_with_retry.return_value = mock_response
        updated_df = data_src._append_gdc_annotations(ann_df)

        # check that the QC populates with 'N' values (indicating no QC failures)
        for c in data_src.KNOWN_QC_CATEGORIES:
            normalized_name = c.replace(' ', '_')
            self.assertTrue(normalized_name in updated_df.columns)
            col_vals = updated_df[normalized_name].values
            self.assertTrue(all(col_vals == 'N'))

    @mock.patch('api.public_data.sources.gdc.tcga.get_with_retry')
    @mock.patch('api.public_data.sources.gdc.tcga.GDCDataSource')
    def test_annotation_data_addition_case2(self, mock_GDCDataSource, mock_get_with_retry):
        '''
        Test that we properly add the annotations when we do not need to paginate results
        '''
        data_src = TCGAMicroRnaSeqDataSource()

        # override this attribute so we can mock paginated responses below:
        mock_GDCDataSource.PAGE_SIZE=1

        # create a dummy dataframe to append to
        ann_df = pd.DataFrame({'sex':['M','F','F'], 'age':[35,46,54]}, index=['A', 'B', 'C'])

        # first try an empty response:
        mock_response = mock.MagicMock()
        # Now mock a response where we get all results back and do not have to
        # paginate the results (e.g. total < size in the pagination key)
        mock_response_json = {
            'data': {
                'hits': [
                    {
                        'id': '51a41ba4-f8df-5543-94b1-4606f5302919', 
                        'entity_id': 'B', 
                        'category': 'Center QC failed', 

                    }
                ],
                'pagination': {
                    'count': 0, 
                    'total': 1, 
                    'size': 100, 
                    'from': 0, 
                    'sort': '', 
                    'page': 1, 
                    'pages': 1
                }
            }, 
            'warnings': {}
        }
        mock_response.json.return_value = mock_response_json
        mock_get_with_retry.return_value = mock_response

        updated_df = data_src._append_gdc_annotations(ann_df)
        vals = updated_df['Center_QC_failed']
        expected = pd.Series({'A':'N', 'B':'Y', 'C':'N'})
        self.assertTrue((vals==expected).all())

    @mock.patch('api.public_data.sources.gdc.tcga.get_with_retry')
    @mock.patch('api.public_data.sources.gdc.tcga.GDCDataSource')
    def test_annotation_data_addition_case3(self, mock_GDCDataSource, mock_get_with_retry):
        '''
        Test that we properly add annotations when pagination of the results payload
        is required
        '''
        data_src = TCGAMicroRnaSeqDataSource()

        # override this attribute so we can mock paginated responses below:
        mock_GDCDataSource.PAGE_SIZE=1

        # create a dummy dataframe to append to
        ann_df = pd.DataFrame({'sex':['M','F','F'], 'age':[35,46,54]}, index=['A', 'B', 'C'])

        # first try an empty response:
        mock_response = mock.MagicMock()
        # Now mock a response where we have to
        # paginate the results
        mock_response_json_1 = {
            'data': {
                'hits': [
                    {
                        'id': 'UUID1', 
                        'entity_id': 'B', 
                        'category': 'Center QC failed', 

                    }
                ],
                'pagination': {
                    'count': 1, 
                    'total': 2, 
                    'size': 1, 
                    'from': 0, 
                    'sort': '', 
                    'page': 1, 
                    'pages': 2
                }
            }, 
            'warnings': {}
        }
        mock_response_json_2 = {
            'data': {
                'hits': [
                    {
                        'id': 'UUID2', 
                        'entity_id': 'C', 
                        'category': 'Item flagged DNU', 

                    }
                ],
                'pagination': {
                    'count': 1, 
                    'total': 2, 
                    'size': 1, 
                    'from': 1, 
                    'sort': '', 
                    'page': 2, 
                    'pages': 2
                }
            }, 
            'warnings': {}
        }
        mock_response.json.side_effect = [mock_response_json_1, mock_response_json_2]
        mock_get_with_retry.return_value = mock_response

        updated_df = data_src._append_gdc_annotations(ann_df)
        vals = updated_df['Center_QC_failed']
        expected = pd.Series({'A':'N', 'B':'Y', 'C':'N'})
        self.assertTrue((vals==expected).all())

        vals = updated_df['Item_flagged_DNU']
        expected = pd.Series({'A':'N', 'B':'N', 'C':'Y'})
        self.assertTrue((vals==expected).all())

    @mock.patch('api.public_data.sources.gdc.tcga.get_with_retry')
    def test_catch_unexpected_annotation_category(self, mock_get_with_retry):
        '''
        If the GDC data annotations are updated to include new category values
        beyond what we've already seen, check that we catch those values
        and issue an exception
        '''
        data_src = TCGAMicroRnaSeqDataSource()

        mock_response = mock.MagicMock()

        # create a dummy dataframe to append to
        ann_df = pd.DataFrame({'sex':['M','F','F'], 'age':[35,46,54]}, index=['A', 'B', 'C'])
        # finally, mock a response with a new, unexpected category
        mock_response_json = {
            'data': {
                'hits': [
                    {
                        'id': '51a41ba4-f8df-5543-94b1-4606f5302919', 
                        'entity_id': 'B', 
                        'category': 'SOMETHING UNEXPECTED', 

                    }
                ],
                'pagination': {
                    'count': 0, 
                    'total': 1, 
                    'size': 100, 
                    'from': 0, 
                    'sort': '', 
                    'page': 1, 
                    'pages': 1
                }
            }, 
            'warnings': {}
        }
        mock_response.json.return_value = mock_response_json
        mock_get_with_retry.return_value = mock_response

        with self.assertRaisesRegex(Exception, 'SOMETHING UNEXPECTED'):
            data_src._append_gdc_annotations(ann_df)


class TestMethylationMixin(BaseAPITestCase): 

    def test_indexes_only_annotation_file(self):
        '''
        An methylation dataset consists of a metadata file and a beta matrix.
        This verifies that the `get_indexable_files`  method only returns
        the annotation file
        '''

        data_src = MethylationMixin()

        fd = {
            MethylationMixin.ANNOTATION_FILE_KEY: ['/path/to/A.txt'],
            MethylationMixin.BETAS_FILE_KEY:['/path/to/betas.tsv'] 
        }
        result = data_src.get_indexable_files(fd)
        self.assertCountEqual(result, fd[MethylationMixin.ANNOTATION_FILE_KEY])

    @mock.patch('api.public_data.sources.methylation.uuid')
    def test_filters_hdf_correctly(self, mock_uuid_mod):
        '''
        Tests that we filter properly for a 
        dummy dataset stored in HDF5 format.
        '''
        hdf_path = os.path.join(
            THIS_DIR, 
            'public_data_test_files', 
            'tcga_methylation.hd5'
        )

        ann_path = os.path.join(
            THIS_DIR, 
            'public_data_test_files', 
            'tcga_methylation_ann.csv'
        )

        # create 5 mock UUIDs. The first two are used in the 
        # first call to the tested method. The final 3 are used in the second
        # call to the tested method. The reason for that is we auto-generate
        # the output filename when the calling function has not provided an 
        # `output_name` arg to the method. In the first call to the tested
        # method, we provide that name, so only two calls are made to the 
        # uuid.uuid4 function. In the second call, we omit that arg and we 
        # hence make an extra call to the uuid4 func.
        mock_uuids = [uuid.uuid4() for i in range(5)]
        mock_uuid_mod.uuid4.side_effect = mock_uuids

        # this dict is what the database record is expected to contain
        # in the file_mapping field
        mock_mapping = {
            # this key doesn't matter- we just include it as a correct
            MethylationMixin.ANNOTATION_FILE_KEY: [ann_path],
            MethylationMixin.BETAS_FILE_KEY:[hdf_path] 

        }
        mock_db_record = mock.MagicMock()
        mock_db_record.file_mapping = mock_mapping
        query = {
            MethylationMixin.SELECTION_KEY:{
                'TCGA-ABC': ['s1', 's3'],
                'TCGA-DEF': ['s5']
            }
        }

        expected_df = pd.DataFrame(
            [[0.64, 0.99, 0.67],[0.29, 0.18,0.29],[0.94,0.41,0.37]],
            index = ['cg1', 'cg2', 'cg3'],
            columns = ['s1','s3','s5']
        )
        ann_df = pd.DataFrame(
            [['TCGA-ABC', 1990],['TCGA-ABC', 1992], ['TCGA-DEF', 1994]],
            index = ['s1','s3','s5'],
            columns = ['cancer_type', 'year_of_birth']
        )

        data_src = MethylationMixin()
        # the children classes will have a TAG attribute. Since we are
        # testing this mixin here, we simply patch it
        tag = 'foo'
        data_src.TAG = tag
        output_name = 'abc'
        # also need to patch the mixin class with a method that would normally
        # be part of the actual dataset class
        mock_apply_additional_filters = mock.MagicMock()
        mock_apply_additional_filters.return_value = (ann_df, expected_df)
        data_src.apply_additional_filters = mock_apply_additional_filters
        paths, filenames, resource_types, file_formats = data_src.create_from_query(mock_db_record, query, output_name)

        mock_apply_additional_filters.assert_called()

        # The order of these doesn't matter in practice, but to check the file contents,
        # we need to be sure we're looking at the correct files for this test.
        self.assertTrue(resource_types[0] == 'MTX')
        self.assertTrue(resource_types[1] == 'ANN')
        self.assertCountEqual(file_formats, [TSV_FORMAT, TSV_FORMAT])

        actual_df = pd.read_table(paths[0], index_col=0)
        self.assertTrue(actual_df.equals(expected_df))


        actual_df = pd.read_table(paths[1], index_col=0)
        self.assertTrue(actual_df.equals(ann_df))

        self.assertEqual(filenames[0], '{x}_beta_values.{t}.tsv'.format(x=output_name, t=tag))
        self.assertEqual(filenames[1], '{x}_ann.{t}.tsv'.format(x=output_name, t=tag))

        # use index 4 below as 2 uuid.uuid4 calls were 'consumed' by the first call to `create_from_query`
        # while the second call  (the one we are testing now) uses 3 calls to the mock UUID method
        # since there the `output_name` arg was not supplied
        paths, filenames, resource_types, file_formats = data_src.create_from_query(mock_db_record, query)
        self.assertEqual(filenames[0], '{t}_beta_values.{u}.tsv'.format(u=mock_uuids[4], t=tag))
        self.assertEqual(filenames[1], '{t}_ann.{u}.tsv'.format(u=mock_uuids[4], t=tag))
        self.assertCountEqual(file_formats, [TSV_FORMAT, TSV_FORMAT])

    def test_rejects_whole_dataset_with_null_filter(self):
        '''
        Tests that we reject the request (raise an exception)
        if a filter of None is applied. This would be too large 
        for us to handle.
        '''
        hdf_path = os.path.join(
            THIS_DIR, 
            'public_data_test_files', 
            'tcga_methylation.hd5'
        )

        # this dict is what the database record is expected to contain
        # in the file_mapping field
        mock_mapping = {
            # this key doesn't matter- we just include it as a correct
            # representation of the database record
            MethylationMixin.ANNOTATION_FILE_KEY: ['/dummy.tsv'],
            MethylationMixin.BETAS_FILE_KEY:[hdf_path] 

        }
        mock_db_record = mock.MagicMock()
        mock_db_record.file_mapping = mock_mapping
        data_src = MethylationMixin()
        data_src.PUBLIC_NAME = 'foo' # the actual implementing class would define this attr typically
        with self.assertRaisesRegex(Exception, 'too large'):
            path, resource_type = data_src.create_from_query(mock_db_record, None)

    def test_filters_with_cancer_type(self):
        '''
        Tests that we handle a bad group ID appropriately
        '''
        hdf_path = os.path.join(
            THIS_DIR, 
            'public_data_test_files', 
            'tcga_methylation.hd5'
        )

        # this dict is what the database record is expected to contain
        # in the file_mapping field
        mock_mapping = {
            # this key doesn't matter- we just include it as a correct
            # representation of the database record
            MethylationMixin.ANNOTATION_FILE_KEY: ['/dummy.tsv'],
            MethylationMixin.BETAS_FILE_KEY:[hdf_path] 

        }
        mock_db_record = mock.MagicMock()
        mock_db_record.file_mapping = mock_mapping
        query = {
            MethylationMixin.SELECTION_KEY:{
                # the only datasets in the hdf5 file are for TCGA-ABC
                # and TCGA-DEF. Below, we ask for a non-existant one
                'TCGA-ABC': ['s1', 's3'],
                'TCGA-XYZ': ['s5']
            }
        }
        data_src = MethylationMixin()
        with self.assertRaisesRegex(Exception, 'TCGA-XYZ'):
            paths, resource_types = data_src.create_from_query(mock_db_record, query)

    def test_filters_with_bad_sample_id(self):
        '''
        Tests that we handle missing samples appropriately
        '''
        hdf_path = os.path.join(
            THIS_DIR, 
            'public_data_test_files', 
            'tcga_methylation.hd5'
        )

        # this dict is what the database record is expected to contain
        # in the file_mapping field
        mock_mapping = {
            # this key doesn't matter- we just include it as a correct
            # representation of the database record
            MethylationMixin.ANNOTATION_FILE_KEY: ['/dummy.tsv'],
            MethylationMixin.BETAS_FILE_KEY:[hdf_path] 

        }
        mock_db_record = mock.MagicMock()
        mock_db_record.file_mapping = mock_mapping
        query = {
            MethylationMixin.SELECTION_KEY:{
                # add a bad sample ID to the TCGA-ABC set:
                'TCGA-ABC': ['s1111', 's3'],
                'TCGA-DEF': ['s5']
            }
        }
        data_src = MethylationMixin()
        with self.assertRaisesRegex(Exception, 's1111'):
            paths, resource_types = data_src.create_from_query(mock_db_record, query)

    def test_empty_filters(self):
        '''
        Tests that we reject if the filtering list is empty
        '''
        hdf_path = os.path.join(
            THIS_DIR, 
            'public_data_test_files', 
            'tcga_methylation.hd5'
        )

        # this dict is what the database record is expected to contain
        # in the file_mapping field
        mock_mapping = {
            # this key doesn't matter- we just include it as a correct
            # representation of the database record
            MethylationMixin.ANNOTATION_FILE_KEY: ['/dummy.tsv'],
            MethylationMixin.BETAS_FILE_KEY:[hdf_path] 

        }
        mock_db_record = mock.MagicMock()
        mock_db_record.file_mapping = mock_mapping
        query = {
            MethylationMixin.SELECTION_KEY:{
                # This should have some strings:
                'TCGA-DEF': []
            }
        }
        data_src = MethylationMixin()
        with self.assertRaisesRegex(Exception, 'empty'):
            paths, names, resource_types = data_src.create_from_query(mock_db_record, query)

    def test_malformatted_filter_dict(self):
        '''
        Tests that we reject if the cancer type refers to something
        that is NOT a list
        '''
        hdf_path = os.path.join(
            THIS_DIR, 
            'public_data_test_files', 
            'tcga_methylation.hd5'
        )

        # this dict is what the database record is expected to contain
        # in the file_mapping field
        mock_mapping = {
            # this key doesn't matter- we just include it as a correct
            # representation of the database record
            MethylationMixin.ANNOTATION_FILE_KEY: ['/dummy.tsv'],
            MethylationMixin.BETAS_FILE_KEY:[hdf_path] 

        }
        mock_db_record = mock.MagicMock()
        mock_db_record.file_mapping = mock_mapping
        query = {
            MethylationMixin.SELECTION_KEY:{
                # This should be a list:
                'TCGA-DEF':'abc'
            }
        }
        data_src = MethylationMixin()
        # again, the children will provide an EXAMPLE_PAYLOAD attribute
        # which we patch into this mixin class here
        data_src.EXAMPLE_PAYLOAD = {
        'TCGA-UVM': ["<UUID>","<UUID>"],
        'TCGA-MESO': ["<UUID>","<UUID>", "<UUID>"]
        }
        with self.assertRaisesRegex(Exception, 'a list of sample identifiers'):
            paths, resource_types = data_src.create_from_query(mock_db_record, query)

    def test_missing_selection_key(self):
        '''
        Tests that we properly warn if the format of the request was incorrect.
        Here, we expect that the chosen samples are addressed by 
        MethylationMixin.SELECTION_KEY. We leave that out here. So even though
        the data structure itself is fine, the overall payload is malformatted
        '''
        hdf_path = os.path.join(
            THIS_DIR, 
            'public_data_test_files', 
            'tcga_methylation.hd5'
        )
        ann_path = os.path.join(
            THIS_DIR, 
            'public_data_test_files', 
            'tcga_methylation_ann.csv'
        )

        # this dict is what the database record is expected to contain
        # in the file_mapping field
        mock_mapping = {
            # this key doesn't matter- we just include it as a correct
            # representation of the database record
            MethylationMixin.ANNOTATION_FILE_KEY: [ann_path],
            MethylationMixin.BETAS_FILE_KEY:[hdf_path] 

        }
        mock_db_record = mock.MagicMock()
        mock_db_record.file_mapping = mock_mapping
        query = {
            'TCGA-ABC': ['s1', 's3'],
            'TCGA-DEF': ['s5']
        }
        
        data_src = MethylationMixin()

        expected_err = 'please pass an object addressed by selections'
        with self.assertRaisesRegex(Exception, expected_err):
            paths, resource_types = data_src.create_from_query(mock_db_record, query)


class TestTCGAMethylation(BaseAPITestCase): 
    '''
    Tests the specialized methods of the class which handles
    TCGA methylation datasets
    '''

    def test_proper_filters_created(self):
        '''
        Tests that the json payload for a metadata
        query is created as expected
        '''
        ds = TCGAMethylationDataSource()
        d = ds._create_methylation_query_params('TCGA-FOO')
        expected_query_filters = {
            "fields": "file_id,file_name,cases.project.program.name,cases.case_id,cases.aliquot_ids,cases.samples.portions.analytes.aliquots.aliquot_id",
            "format": "JSON",
            "size": "100",
            "expand": "cases.demographic,cases.diagnoses,cases.exposures,cases.tissue_source_site,cases.project",
            "filters": "{\"op\": \"and\", \"content\": [{\"op\": \"in\", \"content\": {\"field\": \"files.cases.project.project_id\", \"value\": [\"TCGA-FOO\"]}}, {\"op\": \"in\", \"content\": {\"field\": \"files.analysis.workflow_type\", \"value\": [\"SeSAMe Methylation Beta Estimation\"]}}, {\"op\": \"in\", \"content\": {\"field\": \"files.experimental_strategy\", \"value\": [\"Methylation Array\"]}}, {\"op\": \"in\", \"content\": {\"field\": \"files.data_type\", \"value\": [\"Methylation Beta Value\"]}}]}"
            }
        self.assertDictEqual(d, expected_query_filters)      

    @mock.patch('api.public_data.sources.methylation.MethylationMixin.BETAS_OUTPUT_FILE_TEMPLATE', '__TEST__betas.{tag}.{date}.hd5')
    def test_counts_merged_correctly(self):
        file_to_aliquot_mapping = {
            '1c973626-74ac-4bac-b206-f4e1c2242464': 'x1',
            'e373b043-d716-48cb-b3f1-2afca582b510': 'x2',
            '061de903-e715-485c-9314-fb4794d71bdc': 'x3'
        }
        expected_matrix = pd.DataFrame(
            [[0.2, 0.11, 0.5],[0.3,0.21,0.51],[0.4, 0.31, 0.52]],
            index=['cg1','cg2','cg3'],
            columns = ['x1', 'x2', 'x3']
        )
        expected_matrix.index.name = 'cpg_site'

        archives = [
            os.path.join(THIS_DIR, 'public_data_test_files', 'methylation_archive1.tar.gz'),
            os.path.join(THIS_DIR, 'public_data_test_files', 'methylation_archive2.tar.gz')
        ]

        data_src = TCGAMethylationDataSource()
        # The derived classes will have a ROOT_DIR attribute, but
        # this mixin class doesn't. Patch it here
        data_src.ROOT_DIR = '/tmp'
        actual_df = data_src._merge_downloaded_archives(archives, file_to_aliquot_mapping)
        self.assertTrue(expected_matrix.equals(actual_df))


class TestGDCMethylationDataSourceMixin(BaseAPITestCase):

    def test_parses_hm450_probe_mapping(self):
        '''
        Given a truncated example of the HM450k probe
        annotation file, assert that it has the expected
        format after we pre-process it.
        '''
        # by default, this member refers to a url which is downloaded
        # and parsed. Here we have a shorter version of the same file
        fp = os.path.join(THIS_DIR, 'public_data_test_files', 'example_probes.csv')
        GDCMethylationDataSourceMixin.PROBE_ANN_FILE = fp

        # return all regions:
        target_regions = GDCMethylationDataSourceMixin.GENIC_FEATURE_SET
        result = GDCMethylationDataSourceMixin._prepare_mapping(target_regions)
        expected_result = pd.DataFrame([
            ['EWSR1',   '5p_UTR',  'cg27416437'],
            ['EWSR1',  '1stExon',  'cg27416437'],
            ['RHBDD3',   'TSS200',  'cg27416437'],
            ['ZCCHC12',   'TSS200',  'cg00018261'],
            ['TSPY4',     'Body',  'cg00050873'],
            ['FAM197Y2',  'TSS1500',  'cg00050873'],
        ], columns=['gene_id','feature','probe_id'])
        self.assertTrue(result.equals(expected_result))

        target_regions = [GDCMethylationDataSourceMixin.GENE_BODY]
        result = GDCMethylationDataSourceMixin._prepare_mapping(target_regions)
        expected_result = pd.DataFrame([
            ['TSPY4',     'Body',  'cg00050873'],
        ], columns=['gene_id','feature','probe_id'])
        self.assertTrue(result.equals(expected_result))

        target_regions = [
            GDCMethylationDataSourceMixin.GENE_BODY, 
            GDCMethylationDataSourceMixin.TSS_200
        ]
        result = GDCMethylationDataSourceMixin._prepare_mapping(target_regions)
        expected_result = pd.DataFrame([
            ['RHBDD3',   'TSS200',  'cg27416437'],
            ['ZCCHC12',   'TSS200',  'cg00018261'],
            ['TSPY4',     'Body',  'cg00050873'],
        ], columns=['gene_id','feature','probe_id'])
        self.assertTrue(result.equals(expected_result))

class TestAggregatedGDCMethylation(BaseAPITestCase):

    def setUp(self):
        # create a mock probe-to-gene mapping
        self.mapping = pd.DataFrame(
            [
                ['cg0', 'TSS200', 'gA'],
                ['cg1', '5p_UTR', 'gA'],
                ['cg2', 'Body', 'gA'],
                ['cg3', 'Body', 'gB'],
                ['cg4', 'Body', 'gB'],
                ['cg5', 'Body', 'gB'],
                ['cg6', 'TSS200', 'gC'],
                ['cg7', '1stExon', 'gC'],
                ['cg8', 'Body', 'gC'],
            ],
            columns=[
                GDCMethylationDataSourceMixin.PROBE_ID, 
                GDCMethylationDataSourceMixin.FEATURE_COL,
                GDCMethylationDataSourceMixin.GENE_ID
            ]
        )

        # create a probe-level matrix
        probe_vals = [
            [0.1, 0.2, 0.3],
            [0.4, 0.5, 0.6],
            [0.7, 0.8, 0.9],
            [0.11, 0.21, 0.31],
            [0.41, 0.51, 0.61],
            [0.71, 0.81, 0.91],
            [0.12, 0.22, 0.32],
            [0.42, 0.52, 0.62],
            [0.72, 0.82, 0.92],
        ]
        self.probe_df = pd.DataFrame(
            probe_vals,
            index=[f'cg{x}' for x in range(9)],
            columns=['sA','sB','sC']
        )

    @mock.patch('api.public_data.sources.gdc.gdc.GDCMethylationDataSourceMixin')
    def test_aggregation(self, mock_GDCMethylationDataSourceMixin):
        mock_GDCMethylationDataSourceMixin.PROBE_ID = 'probe_id'
        mock_GDCMethylationDataSourceMixin.GENE_ID = 'gene_id'

        # in our setup, this doesn't actually aggregate since gA and gC both have
        # only a single probe in TSS200 region
        target_features = ['TSS200']
        mapping = self.mapping.loc[
            self.mapping[GDCMethylationDataSourceMixin.FEATURE_COL].isin(target_features)]
        mock_GDCMethylationDataSourceMixin._prepare_mapping.return_value = mapping
        mixin_obj = GDCMethylationAggregationMixin()
        result = mixin_obj._aggregate_probes(self.probe_df, target_features)
        expected_result = pd.DataFrame(
            [
                [0.1, 0.2, 0.3],
                [0.12, 0.22, 0.32]
            ],
            index=['gA', 'gC'],
            columns=['sA','sB','sC']
        )
        self.assertTrue(result.equals(expected_result))

        # gA and gC both have TSS200; gA has a 5'UTR; gC has a 1st exon
        # This will involve getting the mean of those multiple probes
        target_features = ['TSS200', '5p_UTR', '1stExon']
        mapping = self.mapping.loc[
            self.mapping[GDCMethylationDataSourceMixin.FEATURE_COL].isin(target_features)]
        mock_GDCMethylationDataSourceMixin._prepare_mapping.return_value = mapping
        mixin_obj = GDCMethylationAggregationMixin()
        result = mixin_obj._aggregate_probes(self.probe_df, target_features)
        expected_result = pd.DataFrame(
            [
                [0.25, 0.35, 0.45],
                [0.27, 0.37, 0.47]
            ],
            index=['gA', 'gC'],
            columns=['sA','sB','sC'],
            
        )
        self.assertTrue(np.allclose(result.values, expected_result.values))

        # gA and gC both have only a single probe in the body; 
        # gB has three
        # This will involve getting the mean of those multiple probes
        target_features = ['Body']
        mapping = self.mapping.loc[
            self.mapping[GDCMethylationDataSourceMixin.FEATURE_COL].isin(target_features)]
        mock_GDCMethylationDataSourceMixin._prepare_mapping.return_value = mapping
        mixin_obj = GDCMethylationAggregationMixin()
        result = mixin_obj._aggregate_probes(self.probe_df, target_features)
        expected_result = pd.DataFrame(
            [
                [0.7, 0.8, 0.9],
                [0.41, 0.51, 0.61],
                [0.72, 0.82, 0.92]
            ],
            index=['gA', 'gB', 'gC'],
            columns=['sA','sB','sC'],
            
        )
        self.assertTrue(np.allclose(result.values, expected_result.values))

class TestTCGABodyMethylationDataSource(BaseAPITestCase):

    def test_agg(self):
        fp = os.path.join(THIS_DIR, 'public_data_test_files', 'example_probes.csv')
        GDCMethylationDataSourceMixin.PROBE_ANN_FILE = fp
        ds = TCGABodyMethylationDataSource()

        # for this, we do a "full" test which includes
        # parsing the mock HM450k annotation file. Hence,
        # these mock probe vals and probe IDs need to align
        # with that file.
        probe_vals = [
            [0.1, 0.2, 0.3],
            [0.4, 0.5, 0.6],
            [0.7, 0.8, 0.9]
        ]
        probe_ids = [        
            'cg27416437',
            'cg00018261',
            'cg00050873'
        ]
        probe_df = pd.DataFrame(
            probe_vals,
            index=probe_ids,
            columns=['sA','sB','sC']
        )
        result = ds._post_process_betas(probe_df)

        # the only body-based probe in our mock
        # dataset is for TSPY4 (probe cg00050873)
        expected_result = pd.DataFrame(
            [
                [0.7, 0.8, 0.9],
            ],
            index=['TSPY4'],
            columns=['sA','sB','sC'],
            
        )
        self.assertTrue(result.equals(expected_result))        


class TestTCGAPromoterMethylationDataSource(BaseAPITestCase):

    def test_agg(self):
        fp = os.path.join(THIS_DIR, 'public_data_test_files', 'example_probes.csv')
        GDCMethylationDataSourceMixin.PROBE_ANN_FILE = fp
        ds = TCGAPromoterMethylationDataSource()

        # for this, we do a "full" test which includes
        # parsing the mock HM450k annotation file. Hence,
        # these mock probe vals and probe IDs need to align
        # with that file.
        probe_vals = [
            [0.1, 0.2, 0.3],
            [0.4, 0.5, 0.6],
            [0.7, 0.8, 0.9]
        ]
        probe_ids = [        
            'cg27416437',
            'cg00018261',
            'cg00050873'
        ]
        probe_df = pd.DataFrame(
            probe_vals,
            index=probe_ids,
            columns=['sA','sB','sC']
        )
        result = ds._post_process_betas(probe_df)
        
        # the same probe covers both EWSR1 (5'UTR, 1st exon)
        # and RHBDD3 (TSS200). Probe cg00018261 samples in 
        # TSS200 of ZCCHC12
        expected_result = pd.DataFrame(
            [
            [0.1, 0.2, 0.3],
            [0.1, 0.2, 0.3],
            [0.4, 0.5, 0.6]
            ],
            index=['EWSR1', 'RHBDD3', 'ZCCHC12'],
            columns=['sA','sB','sC'],
            
        )
        self.assertTrue(result.equals(expected_result)) 