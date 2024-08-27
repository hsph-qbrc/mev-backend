import unittest.mock as mock
import os
from io import BytesIO

import pandas as pd
import numpy as np

from django.urls import reverse
from django.core.files import File
from django.contrib.auth import get_user_model
from django.core.exceptions import ImproperlyConfigured
from rest_framework import status

from constants import TSV_FORMAT,\
    MATRIX_KEY,\
    BED3_FILE_KEY,\
    BED6_FILE_KEY,\
    NARROWPEAK_FILE_KEY, \
    FASTA_KEY, \
    FASTA_FORMAT, \
    FIRST_COLUMN_ID
from resource_types import RESOURCE_MAPPING
from resource_types.table_types import PREVIEW_NUM_LINES
from api.models import Resource

from api.tests.base import BaseAPITestCase
from api.tests.test_helpers import associate_file_with_resource

TESTDIR = os.path.dirname(__file__)
TESTDIR = os.path.join(TESTDIR, 'resource_validation_test_files')


class TestResourcePreviewEndpoint(BaseAPITestCase):
    '''
    Tests that the resource previews return the proper
    format.
    '''

    def setUp(self):

        self.establish_clients()

        # get an example from the database:
        regular_user_resources = Resource.objects.filter(
            owner=self.regular_user_1,
        )
        if len(regular_user_resources) == 0:
            msg = '''
                Testing not setup correctly.  Please ensure that there is at least one
                Resource instance for the user {user}
            '''.format(user=self.regular_user_1)
            raise ImproperlyConfigured(msg)
        for r in regular_user_resources:
            if r.is_active:
                active_resource = r
                break
        self.resource = active_resource
        self.url = reverse(
            'resource-preview', 
            kwargs={'pk': self.resource.pk}
        )
        for r in regular_user_resources:
            if not r.is_active:
                inactive_resource = r
                break
        self.inactive_resource_url = reverse(
            'resource-contents', 
            kwargs={'pk':inactive_resource.pk}
        )

    @mock.patch('api.views.resource_views.check_resource_request')
    def test_preview_request(self, mock_check_resource_request):
        f = os.path.join(TESTDIR, 'test_integer_matrix.tsv')
        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = MATRIX_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        mock_check_resource_request.return_value = (True, self.resource)
        response = self.authenticated_regular_client.get(
            self.url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        self.assertTrue(len(j) == PREVIEW_NUM_LINES)

        df = pd.read_table(f, index_col=0, nrows=PREVIEW_NUM_LINES)
        expected_rownames = df.index.tolist()

        returned_rownames = [x[FIRST_COLUMN_ID] for x in j]
        self.assertCountEqual(returned_rownames, expected_rownames)

        # test that we ignore params for the preview:
        url = self.url + '?page_size=20'
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        self.assertTrue(len(j) == PREVIEW_NUM_LINES)
        returned_rownames = [x[FIRST_COLUMN_ID] for x in j]
        self.assertCountEqual(returned_rownames, expected_rownames)

    @mock.patch('api.views.resource_views.check_resource_request')
    def test_preview_request_for_file_without_preview(self, mock_check_resource_request):
        # doesn't matter that the file is not a FASTA- just need something here
        # The key is that we should return prior to even touching the file since
        # the resource type does not support a preview.
        f = os.path.join(TESTDIR, 'test_integer_matrix.tsv')
        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = FASTA_KEY
        self.resource.file_format = FASTA_FORMAT
        self.resource.save()
        mock_check_resource_request.return_value = (True, self.resource)
        response = self.authenticated_regular_client.get(
            self.url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        self.assertTrue('Contents not available' in j['info'])


class TestResourcePreview(BaseAPITestCase):

    @mock.patch('resource_types.table_types.PREVIEW_NUM_LINES', 2)
    def test_table_preview(self):
        '''
        Tests that the returned preview has the expected format.

        Note that to keep things simple, we override the default
        value dictating the number of preview lines.
        '''

        columns = ['colA', 'colB', 'colC']
        rows = ['geneA', 'geneB', 'geneC']
        values = np.arange(9).reshape((3,3))
        full_file_return = [
            {FIRST_COLUMN_ID: 'geneA', 'values': {'colA':0, 'colB':1, 'colC':2}},
            {FIRST_COLUMN_ID: 'geneB', 'values': {'colA':3, 'colB':4, 'colC':5}},
            {FIRST_COLUMN_ID: 'geneC', 'values': {'colA':6, 'colB':7, 'colC':8}}
        ]
        preview_return = [
            {FIRST_COLUMN_ID: 'geneA', 'values': {'colA':0, 'colB':1, 'colC':2}},
            {FIRST_COLUMN_ID: 'geneB', 'values': {'colA':3, 'colB':4, 'colC':5}},
        ]
        df = pd.DataFrame(values, index=rows, columns=columns)
        path = os.path.join('/tmp', 'test_preview_matrix.tsv')
        df.to_csv(path, sep='\t')
        r = Resource.objects.all()[0]
        # need to set the resource type since files that we can preview 
        # have been validated
        r.resource_type = MATRIX_KEY
        r.file_format = TSV_FORMAT
        r.save()
        associate_file_with_resource(r, path)
        mtx_class = RESOURCE_MAPPING[MATRIX_KEY]
        mtx_type = mtx_class()

        # check that default of preview=False (no passed preview arg)
        # returns the full 3 lines:
        contents = mtx_type.get_contents(r)
        contents = mtx_type.to_json(contents)
        self.assertCountEqual(contents, full_file_return)

        # with the preview arg, check that we only get two lines back:
        contents = mtx_type.get_contents(r, preview=True)
        contents = mtx_type.to_json(contents)
        self.assertCountEqual(contents, preview_return)
        os.remove(path)

    def test_empty_table_preview(self):
        '''
        In principle, the resource should be validated so this should
        never happen.  But just in case, we test what happens if the table
        is empty or has a parsing error
        '''
        path = os.path.join(TESTDIR, 'test_empty.tsv')

        mtx_class = RESOURCE_MAPPING[MATRIX_KEY]
        mtx_type = mtx_class()
        with self.assertRaises(Exception):
            mtx_type.get_contents(path, 'tsv')

    @mock.patch('resource_types.table_types.PREVIEW_NUM_LINES', 2)
    def test_bed3_file_preview(self):

        resource_path = os.path.join(TESTDIR, 'example_bed.bed')
        r = Resource.objects.create(
            datafile=File(BytesIO(), 'foo.tsv'),
            name='foo.tsv',
            resource_type=BED3_FILE_KEY,
            file_format=TSV_FORMAT,
            owner=get_user_model().objects.all()[0]
        )
        associate_file_with_resource(r, resource_path)

        bed3_class = RESOURCE_MAPPING[BED3_FILE_KEY]
        bed3_type = bed3_class()

        # check that default of preview=False (no passed preview arg)
        # returns the full 3 lines:
        contents = bed3_type.get_contents(r)
        contents = bed3_type.to_json(contents)

        full_file_return = [
            {FIRST_COLUMN_ID: 0, 'values': {'chrom':'chr1', 'start':100, 'stop':200}},
            {FIRST_COLUMN_ID: 1, 'values': {'chrom':'chr1', 'start':200, 'stop':340}},
            {FIRST_COLUMN_ID: 2, 'values': {'chrom':'chrX', 'start':100, 'stop':200}}
        ]
        self.assertCountEqual(contents, full_file_return)
        # with the preview arg, check that we only get two lines back:
        contents = bed3_type.get_contents(r, preview=True)
        contents = bed3_type.to_json(contents)
        preview_return = [
            {FIRST_COLUMN_ID: 0, 'values': {'chrom':'chr1', 'start':100, 'stop':200}},
            {FIRST_COLUMN_ID: 1, 'values': {'chrom':'chr1', 'start':200, 'stop':340}}
        ]
        self.assertCountEqual(contents, preview_return)

    @mock.patch('resource_types.table_types.PREVIEW_NUM_LINES', 2)
    def test_bed6_file_preview(self):

        resource_path = os.path.join(TESTDIR, 'bed6_example.bed')
        r = Resource.objects.create(
            datafile = File(BytesIO(), 'foo.tsv'),
            name = 'foo.tsv',
            resource_type = BED6_FILE_KEY,
            file_format = TSV_FORMAT,
            owner = get_user_model().objects.all()[0]
        )
        associate_file_with_resource(r, resource_path)

        bed6_class = RESOURCE_MAPPING[BED6_FILE_KEY]
        bed6_type = bed6_class()

        # check that default of preview=False (no passed preview arg)
        # returns the full 3 lines:
        contents = bed6_type.get_contents(r)
        contents = bed6_type.to_json(contents)
        full_file_return = [
            {FIRST_COLUMN_ID: 0, 'values': {'chrom':'chr1', 'start':100, 'stop':200, 'name':'gA', 'score': 100, 'strand': '.'}},
            {FIRST_COLUMN_ID: 1, 'values': {'chrom':'chr1', 'start':200, 'stop':340, 'name':'gB', 'score': 100, 'strand': '.'}},
            {FIRST_COLUMN_ID: 2, 'values': {'chrom':'chrX', 'start':100, 'stop':200, 'name':'gC', 'score': 100, 'strand': '.'}}
        ]
        self.assertCountEqual(contents, full_file_return)
        # with the preview arg, check that we only get two lines back:
        contents = bed6_type.get_contents(r, preview=True)
        contents = bed6_type.to_json(contents)
        preview_return = [
            {FIRST_COLUMN_ID: 0, 'values': {'chrom':'chr1', 'start':100, 'stop':200, 'name':'gA', 'score': 100, 'strand': '.'}},
            {FIRST_COLUMN_ID: 1, 'values': {'chrom':'chr1', 'start':200, 'stop':340, 'name':'gB', 'score': 100, 'strand': '.'}}
        ]
        self.assertCountEqual(contents, preview_return)

    @mock.patch('resource_types.table_types.PREVIEW_NUM_LINES', 2)
    def test_narrowpeak_file_preview(self):

        resource_path = os.path.join(TESTDIR, 'narrowpeak_example.bed')
        r = Resource.objects.create(
            datafile = File(BytesIO(), 'foo.tsv'),
            name = 'foo.tsv',
            resource_type = BED6_FILE_KEY,
            file_format = TSV_FORMAT,
            owner = get_user_model().objects.all()[0]
        )
        associate_file_with_resource(r, resource_path)

        np_class = RESOURCE_MAPPING[NARROWPEAK_FILE_KEY]
        np_type = np_class()

        # check that default of preview=False (no passed preview arg)
        # returns the full 3 lines:
        contents = np_type.get_contents(r)
        contents = np_type.to_json(contents)
        full_file_return = [
            {
                FIRST_COLUMN_ID: 0, 
                'values': {
                    'chrom':'chr1', 
                    'start':100, 
                    'stop':200, 
                    'name':'gA', 
                    'score': 100, 
                    'strand': '.',
                    'signal_value': 0,
                    'pval': 23.1,
                    'qval': 12.43,
                    'peak': -1
                }
            },
            {
                FIRST_COLUMN_ID: 1, 
                'values': {
                    'chrom':'chr1', 
                    'start':200, 
                    'stop':340, 
                    'name':'gB', 
                    'score': 100, 
                    'strand': '.',
                    'signal_value': 0,
                    'pval': 2.1,
                    'qval': 0.2,
                    'peak': -1
                }
            },
            {
                FIRST_COLUMN_ID: 2, 
                'values': {
                    'chrom':'chrX', 
                    'start':100, 
                    'stop':200, 
                    'name':'gC', 
                    'score': 100, 
                    'strand': '.',
                    'signal_value': 0,
                    'pval': 34.2,
                    'qval': 14.2,
                    'peak': -1
                }
            }
        ]
        self.assertCountEqual(contents, full_file_return)
        # with the preview arg, check that we only get two lines back:
        contents = np_type.get_contents(r, preview=True)
        contents = np_type.to_json(contents)
        preview_return = [
            {
                FIRST_COLUMN_ID: 0, 
                'values': {
                    'chrom':'chr1', 
                    'start':100, 
                    'stop':200, 
                    'name':'gA', 
                    'score': 100, 
                    'strand': '.',
                    'signal_value': 0,
                    'pval': 23.1,
                    'qval': 12.43,
                    'peak': -1
                }
            },
            {
                FIRST_COLUMN_ID: 1, 
                'values': {
                    'chrom':'chr1', 
                    'start':200, 
                    'stop':340, 
                    'name':'gB', 
                    'score': 100, 
                    'strand': '.',
                    'signal_value': 0,
                    'pval': 2.1,
                    'qval': 0.2,
                    'peak': -1
                }
            }
        ]
        self.assertCountEqual(contents, preview_return)