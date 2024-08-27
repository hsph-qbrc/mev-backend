import os
import unittest.mock as mock
import json
from itertools import chain

import pandas as pd
import numpy as np
from networkx import Graph

from django.conf import settings

from constants import MATRIX_KEY, \
    TSV_FORMAT, \
    FEATURE_TABLE_KEY, \
    JSON_FILE_KEY, \
    JSON_FORMAT, \
    POSITIVE_MARKER, \
    NEGATIVE_MARKER, \
    FIRST_COLUMN_ID

from exceptions import ParseException

from resource_types import get_resource_type_instance

from api.models import Resource
from api.tests.base import BaseAPITestCase
from api.tests.test_helpers import associate_file_with_resource

from api.data_transformations.network_transforms import subset_PANDA_net, \
    subset_full_network, \
    get_result_matrices, \
    filter_by_significance, \
    add_edges, \
    walk_for_neighbors, \
    add_top_neighbor_nodes, \
    max_edge_subsetting, \
    max_weight_subsetting, \
    node_list_subsetting, \
    format_response_graph  
from api.data_transformations.heatmap_transforms import heatmap_reduce, \
    heatmap_cluster, \
    perform_clustering
from api.data_transformations.volcano_plot_transforms import volcano_subset


class ResourceTransformTests(BaseAPITestCase):

    def setUp(self):

        self.TESTDIR = os.path.join(
            os.path.dirname(__file__),
            'resource_contents_test_files'    
        )
        # get an example from the database:
        self.resource = Resource.objects.all()[0]

    def test_clustering_function(self):
        '''
        Tests the clustering function on the entire matrix
        '''
        fp = os.path.join(self.TESTDIR, 'heatmap_hcl_test.tsv')
        self.resource.resource_type = MATRIX_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        associate_file_with_resource(self.resource, fp)
        resource_type_instance = get_resource_type_instance(MATRIX_KEY)
        df = resource_type_instance.get_contents(self.resource)
        result = perform_clustering(df, 'ward', 'euclidean', resource_type_instance)
        expected_row_ordering = ['g5','g1','g3','g6','g2','g4']
        self.assertEqual(expected_row_ordering, [x[FIRST_COLUMN_ID] for x in result])
        expected_col_ordering = ['s1','s3','s5','s2','s4','s6']
        self.assertEqual(expected_col_ordering, [x for x in result[0]['values']])

    def test_clustering_function_on_empty(self):
        '''
        Tests the clustering function on a rowname filter that
        returns no records
        '''
        fp = os.path.join(self.TESTDIR, 'heatmap_hcl_test.tsv')
        self.resource.resource_type = MATRIX_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        associate_file_with_resource(self.resource, fp)
        df = pd.read_table(fp, index_col=0)
        resource_type_instance = get_resource_type_instance(MATRIX_KEY)

        query_params = {
            # This will cause an empty dataframe since there are no rows
            # with that gene symbol
            settings.ROWNAME_FILTER: '[in]:ABC'
        }
        # note that we don't do df.loc['g1'] since that creates a pd.Series.
        # instead, we rely on the actual function that filters the dataframe
        # which returns a single-row dataframe instead:
        resource_type_instance.get_contents(self.resource, query_params)
        df = resource_type_instance.table
        # create an empty dataframe to see how it handles that
        result = perform_clustering(df, 'ward', 'euclidean', resource_type_instance)
        self.assertTrue(result == [])

    def test_clustering_function_single_record(self):
        '''
        Tests the clustering function on a single gene
        '''
        fp = os.path.join(self.TESTDIR, 'heatmap_hcl_test.tsv')
        self.resource.resource_type = MATRIX_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        associate_file_with_resource(self.resource, fp)
        df = pd.read_table(fp, index_col=0)
        resource_type_instance = get_resource_type_instance(MATRIX_KEY)

        query_params = {
            settings.ROWNAME_FILTER: f'[in]:g1'
        }
        # note that we don't do df.loc['g1'] since that creates a pd.Series.
        # instead, we rely on the actual function that filters the dataframe
        # which returns a single-row dataframe instead:
        resource_type_instance.get_contents(self.resource, query_params)
        df = resource_type_instance.table
        result = perform_clustering(df, 'ward', 'euclidean', resource_type_instance)
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0][FIRST_COLUMN_ID] == 'g1')

    @mock.patch('api.data_transformations.heatmap_transforms.perform_clustering')
    def test_heatmap_cluster_transform_case1(self, mock_perform_clustering):
        '''
        Note that we don't test the clustering function itself-- 
        only the call TO that function
        '''
        fp = os.path.join(self.TESTDIR, 'heatmap_hcl_test.tsv')
        df = pd.read_table(fp, index_col=0)
        keep_genes = 'g1,g2,g3,g4,g6'
        df = df.loc[keep_genes.split(',')]

        self.resource.resource_type = MATRIX_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        associate_file_with_resource(self.resource, fp)
        query_params = {
            'metric': 'euclidean',
            'transform-name': 'blah',
            settings.ROWNAME_FILTER: f'[in]:{keep_genes}' # leaves out g5
        }
        heatmap_cluster(self.resource, query_params)
        args, kwargs = mock_perform_clustering.call_args
        passed_df = args[0]
        passed_method = args[1]
        passed_metric = args[2]
        self.assertTrue((passed_df == df).all().all())
        self.assertTrue(passed_method == 'ward')
        self.assertTrue(passed_metric == 'euclidean')
        self.assertTrue(kwargs == {})

    @mock.patch('api.data_transformations.heatmap_transforms.perform_clustering')
    def test_heatmap_cluster_transform_case2(self, mock_perform_clustering):
        '''
        Here, we pass an extra query param which should cause a problem
        '''
        fp = os.path.join(self.TESTDIR, 'heatmap_hcl_test.tsv')
        df = pd.read_table(fp, index_col=0)
        keep_genes = 'g1,g2,g3,g4,g6'
        df = df.loc[keep_genes.split(',')]

        self.resource.resource_type = MATRIX_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        associate_file_with_resource(self.resource, fp)
        query_params = {
            'metric': 'euclidean',
            'EXTRA': 'foo', # <-- BAD
            'transform-name': 'blah',
            settings.ROWNAME_FILTER: f'[in]:{keep_genes}' # leaves out g5
        }
        with self.assertRaisesRegex(ParseException, 'EXTRA'):
            heatmap_cluster(self.resource, query_params)

    @mock.patch('api.data_transformations.heatmap_transforms.perform_clustering')
    def test_heatmap_cluster_transform_case3(self, mock_perform_clustering):
        '''
        Here, we pass a gene/feature filter that has no items
        '''
        fp = os.path.join(self.TESTDIR, 'heatmap_hcl_test.tsv')
        empty_df = pd.DataFrame(columns=['s1','s2','s3','s4','s5','s6'])

        self.resource.resource_type = MATRIX_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        associate_file_with_resource(self.resource, fp)
        query_params = {
            'metric': 'euclidean',
            'transform-name': 'blah',
            settings.ROWNAME_FILTER: f'[in]:'
        }
        heatmap_cluster(self.resource, query_params)
        args, kwargs = mock_perform_clustering.call_args
        passed_df = args[0]
        passed_method = args[1]
        passed_metric = args[2]
        self.assertTrue((passed_df == empty_df).all().all())
        self.assertTrue(passed_method == 'ward')
        self.assertTrue(passed_metric == 'euclidean')
        self.assertTrue(kwargs == {})

    def test_heatmap_cluster_bad_resource_type(self):
        '''
        Test that we appropriately warn if the heatmap clustering function
        is called for an unacceptable resource type
        '''
        # test that it works for an all-numeric FT:
        fp = os.path.join(self.TESTDIR, 'json_array_file.json')
        self.resource.resource_type = JSON_FILE_KEY
        self.resource.file_format = JSON_FORMAT
        self.resource.save()
        associate_file_with_resource(self.resource, fp)
        query_params = {'transform-name': 'foo'}
        with self.assertRaisesRegex(Exception, 'Not an acceptable resource type'):
            heatmap_cluster(self.resource, query_params)

    def test_heatmap_reduce_transform_case(self):
        fp = os.path.join(self.TESTDIR, 'heatmap_hcl_test.tsv')
        self.resource.resource_type = MATRIX_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        associate_file_with_resource(self.resource, fp)
        query_params = {
            'mad_n': 5
        }
        result = heatmap_reduce(self.resource, query_params)
        expected_row_ordering = ['g3','g1','g2','g6','g4']
        self.assertEqual(expected_row_ordering, [x[FIRST_COLUMN_ID] for x in result])
        expected_col_ordering = ['s1','s3','s5','s2','s4','s6']
        self.assertEqual(expected_col_ordering, [x for x in result[0]['values']])

    def test_heatmap_reduce_bad_resource_type(self):
        '''
        Test that we appropriately warn if the heatmap reduce function
        is called for an unacceptable resource type
        '''
        # test that it works for an all-numeric FT:
        fp = os.path.join(self.TESTDIR, 'json_array_file.json')
        self.resource.resource_type = JSON_FILE_KEY
        self.resource.file_format = JSON_FORMAT
        self.resource.save()
        associate_file_with_resource(self.resource, fp)
        query_params = {
            'mad_n': 5
        }
        with self.assertRaisesRegex(Exception, 'Not an acceptable resource type'):
            heatmap_reduce(self.resource, query_params)

    def test_heatmap_reduce_warns_non_numeric(self):
        '''
        Test that we appropriately warn if the heatmap reduce function
        can't work due to a non-numeric entry in the table. Since we can
        technically have feature tables that are all numeric, they CAN
        work on some types. However, we need to catch and warn attempts
        where we can't perform the calculation. 
        
        An example would be an output from LIONESS--
        it's a feature table since each row concerns the weights
        (e.g. to transcription factors) for each gene. It's NOT
        a matrix since the columns are not technically observations
        (samples).
        '''
        # test that it works for an all-numeric FT:
        fp = os.path.join(self.TESTDIR, 'heatmap_hcl_test.tsv')
        self.resource.resource_type = FEATURE_TABLE_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        associate_file_with_resource(self.resource, fp)
        query_params = {
            'mad_n': 5
        }
        result = heatmap_reduce(self.resource, query_params)
        expected_row_ordering = ['g3','g1','g2','g6','g4']
        self.assertEqual(expected_row_ordering, [x[FIRST_COLUMN_ID] for x in result])
        expected_col_ordering = ['s1','s3','s5','s2','s4','s6']
        self.assertEqual(expected_col_ordering, [x for x in result[0]['values']])

        # now test that it fails for a table with non-numeric
        fp = os.path.join(self.TESTDIR, 'table_with_string_field.tsv')
        self.resource.resource_type = FEATURE_TABLE_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        associate_file_with_resource(self.resource, fp)
        query_params = {
            'mad_n': 5
        }
        with self.assertRaisesRegex(Exception, 'Could not calculate'):
            heatmap_reduce(self.resource, query_params)

    def test_panda_subset_transform_case(self):
        fp = os.path.join(self.TESTDIR, 'example_panda_output.tsv')
        associate_file_with_resource(self.resource, fp)

        expected_result = {
            "initial_axis": 0,
            "nodes": [
                {
                "ENSG_7": {
                    "axis": 0,
                    "children": [
                    {
                        "TF_5": 99
                    },
                    {
                        "TF_3": 98
                    }
                    ]
                }
                },
                {
                "ENSG_6": {
                    "axis": 0,
                    "children": [
                    {
                        "TF_5": 96
                    },
                    {
                        "TF_1": 87
                    }
                    ]
                }
                },
                {
                "TF_1": {
                    "axis": 1,
                    "children": [
                    {
                        "ENSG_2": 97
                    },
                    {
                        "ENSG_6": 87
                    }
                    ]
                }
                },
                {
                "TF_5": {
                    "axis": 1,
                    "children": [
                    {
                        "ENSG_7": 99
                    },
                    {
                        "ENSG_6": 96
                    }
                    ]
                }
                },
                {
                "TF_3": {
                    "axis": 1,
                    "children": [
                    {
                        "ENSG_7": 98
                    },
                    {
                        "ENSG_10": 97
                    }
                    ]
                }
                }
            ]
        }
        expected_nodes = ['ENSG_6', 'ENSG_7', 'TF_1', 'TF_3', 'TF_5']
        query_params = {
            'maxdepth': 2,
            'children': 2,
            'axis': 0
        }
        result = subset_PANDA_net(self.resource, query_params)
        nodes = list(set(chain.from_iterable([x.keys() for x in result['nodes']])))
        self.assertCountEqual(expected_nodes, nodes)

        # # check that incomplete query parameters raise an exception:
        # # missing maxdepth
        query_params = {
            'children': 2,
            'axis': 0
        }
        with self.assertRaisesRegex(Exception, 'maxdepth'):
            subset_PANDA_net(self.resource, query_params)

        # maxdepth should be a positive integer. Here it's negative
        query_params = {
            'maxdepth': -2,
            'children': 2,
            'axis': 0
        }
        with self.assertRaisesRegex(Exception, 'not a positive integer'):
            subset_PANDA_net(self.resource, query_params)

        # axis should be 0 or 1
        query_params = {
            'maxdepth': 2,
            'children': 2,
            'axis': 3
        }
        with self.assertRaisesRegex(Exception, 'must be 0 or 1'):
            subset_PANDA_net(self.resource, query_params)

        

    def test_panda_subset_by_genes(self):
        fp = os.path.join(self.TESTDIR, 'example_panda_output.tsv')
        associate_file_with_resource(self.resource, fp)

        expected_result = {
            "initial_axis": 0,
            "nodes": [
                {
                "ENSG_1": {
                    "axis": 0,
                    "children": [
                    {
                        "TF_2": 80
                    },
                    {
                        "TF_4": 87
                    }
                    ]
                }
                },
                {
                "ENSG_9": {
                    "axis": 0,
                    "children": [
                    {
                        "TF_7": 68
                    },
                    {
                        "TF_4": 73
                    }
                    ]
                }
                },
                {
                "TF_2": {
                    "axis": 1,
                    "children": [
                    {
                        "ENSG_1": 80
                    },
                    {
                        "ENSG_4": 95
                    }
                    ]
                }
                },
                {
                "TF_4": {
                    "axis": 1,
                    "children": [
                    {
                        "ENSG_1": 87
                    },
                    {
                        "ENSG_10": 91
                    }
                    ]
                }
                },
                {
                "TF_7": {
                    "axis": 1,
                    "children": [
                    {
                        "ENSG_5": 89
                    },
                    {
                        "ENSG_2": 90
                    }
                    ]
                }
                }
            ]
        }
        expected_nodes = ['ENSG_1', 'ENSG_9', 'TF_2', 'TF_4', 'TF_7']
        query_params = {
            'maxdepth': 2,
            'children': 2,
            'axis': 0,
            'initial_nodes': 'ENSG_1,ENSG_9'
        }
        result = subset_PANDA_net(self.resource, query_params)
        nodes = list(set(chain.from_iterable([x.keys() for x in result['nodes']])))
        self.assertCountEqual(expected_nodes, nodes)

        # test with a bad gene
        query_params = {
            'maxdepth': 2,
            'children': 2,
            'axis': 0,
            'initial_nodes': 'ENSG_1,ENSG_666'
        }
        with self.assertRaisesRegex(Exception, 'ENSG_666'):
            subset_PANDA_net(self.resource, query_params)

        # test with a bad delimiter
        query_params = {
            'maxdepth': 2,
            'children': 2,
            'axis': 0,
            'initial_nodes': 'ENSG_1:ENSG_666'
        }
        with self.assertRaisesRegex(Exception, 'ENSG_1:ENSG_666'):
            subset_PANDA_net(self.resource, query_params)

        # test other direction (TFs, axis=1), but keep (by accident)
        # a query on the genes
        query_params = {
            'maxdepth': 2,
            'children': 2,
            'axis': 1,
            'initial_nodes': 'ENSG_1,ENSG_9'
        }
        with self.assertRaisesRegex(Exception, 'ENSG_1'):
            result = subset_PANDA_net(self.resource, query_params)

        # now try a correct query for the TFs
        query_params = {
            'maxdepth': 2,
            'children': 2,
            'axis': 1,
            'initial_nodes': 'TF_2,TF_4'
        }
        result = subset_PANDA_net(self.resource, query_params)
        nodes = list(set(chain.from_iterable([x.keys() for x in result['nodes']])))
        expected_nodes = ['TF_2', 'TF_4', 'ENSG_1', 'ENSG_4', 'ENSG_10']
        self.assertCountEqual(expected_nodes, nodes)

        # test that too many requested nodes causes an error
        # The 0 axis has 40 entries. If it had fewer entries (e.g. 10),
        # then the top N function would just select 10 if given a
        # large number
        query_params = {
            'maxdepth': 3,
            'children': 30,
            'axis': 0
        }
        with self.assertRaisesRegex(Exception, 'choose fewer'):
            result = subset_PANDA_net(self.resource, query_params)

    def test_volcano_subset(self):
        '''
        Test that the volcano subset transform works as
        expected
        '''
        # test that it works for an all-numeric FT:
        fp = os.path.join(self.TESTDIR, 'demo_deseq_table_2.tsv')
        self.resource.resource_type = FEATURE_TABLE_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        associate_file_with_resource(self.resource, fp)
        query_params = {
            'pval': 0.03,
            'lfc': 0.5
        }
        result = volcano_subset(self.resource, query_params)
        self.assertTrue(len(result) == 2)

        # with 8 lines in the file (7 data rows), a 'frac'
        # of 0.3 will give 2 'other' rows
        query_params = {
            'pval': 0.03,
            'lfc': 0.5,
            'fraction': 0.3
        }
        result = volcano_subset(self.resource, query_params)
        self.assertTrue(len(result) == 4)
        j = json.dumps(result, indent=2)

        # with these parameters, we don't return any
        # results under that threshold. then, we only
        # return the 'other' 2 results
        query_params = {
            'pval': 0.001,
            'lfc': 0.5,
            'fraction': 0.3
        }
        result = volcano_subset(self.resource, query_params)
        self.assertTrue(len(result) == 2)

        # test that we watch errors:
        query_params = {
            'pval': -0.1, #<-- bad, negative
            'lfc': 0.5,
        }
        with self.assertRaisesRegex(Exception, 'positive float between'):
            volcano_subset(self.resource, query_params)

        query_params = {
            'pval': 0.1,
            'lfc': -0.5, #<-- bad, negative
        }
        with self.assertRaisesRegex(Exception, 'positive float'):
            volcano_subset(self.resource, query_params)

        query_params = {
            'pval': 0.1,
            'lfc': 0.5,
            'fraction': 1.5
        }
        with self.assertRaisesRegex(Exception, 'positive float between'):
            volcano_subset(self.resource, query_params)

        # check that it must be a particular resource type
        self.resource.resource_type = MATRIX_KEY
        self.resource.save()
        query_params = {
            'pval': 0.001,
            'lfc': 0.5,
            'fraction': 0.1
        }
        with self.assertRaisesRegex(Exception, 'Not an acceptable resource type'):
            volcano_subset(self.resource, query_params)

        # check using a file that doesn't have the correct column
        fp = os.path.join(self.TESTDIR, 'demo_file2.tsv')
        self.resource.resource_type = FEATURE_TABLE_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        associate_file_with_resource(self.resource, fp)
        query_params = {
            'pval': 0.03,
            'lfc': 0.5
        }
        with self.assertRaisesRegex(Exception, '"padj" and "log2FoldChange" column'):
            volcano_subset(self.resource, query_params)


class NetworkSubsetTests(BaseAPITestCase):
    '''
    Tests the functions that perform network subsetting
    as provided by a network matrix and matched significance
    matrix.
    '''
    def setUp(self):

        self.TESTDIR = os.path.join(
            os.path.dirname(__file__),
            'resource_contents_test_files'    
        )
        #load data common to the network prioritzation schemes:
        self.adj_mtx_fp = os.path.join(self.TESTDIR, 'demo_correlation_mtx.tsv')
        self.pval_mtx_fp = os.path.join(self.TESTDIR, 'demo_pval_mtx.tsv')
        self.adj_mtx = pd.read_table(self.adj_mtx_fp, index_col=0)
        self.pval_mtx = pd.read_table(self.pval_mtx_fp, index_col=0)

    def test_matrix_filtering(self):
        '''
        This tests that we properly subset
        the connectivity matrix according to the
        significance matrix
        '''
        idx = ['g0', 'g1', 'm0', 'm1', 'm2']
        pc = pd.DataFrame(np.arange(25).reshape((5,5)), index=idx, columns=idx)
        pc = pc + pc.T # so it's symmetric
        sig = pd.DataFrame(np.ones_like(pc), index=idx, columns=idx)
        sig.loc['g1', 'm0'] = 0.01
        sig.loc['m0', 'g1'] = 0.01
        sig.loc['m1', 'm0'] = 0.1
        sig.loc['m0', 'm1'] = 0.1

        adj_mtx = filter_by_significance(pc, sig, 0.2)
        expected = pd.DataFrame(
                    np.array([[np.nan, 18., np.nan],
                             [18., np.nan, 30.],
                             [np.nan, 30., np.nan]]),
                    index=['g1','m0','m1'],
                    columns=['g1','m0','m1'])
        self.assertTrue(expected.equals(adj_mtx))

        # choose a threshold that will return an empty dataframe
        adj_mtx = filter_by_significance(pc, sig, 0.001)
        self.assertTrue(adj_mtx.shape == (0,0))


    def test_handle_empty_matrix(self):
        '''
        If there are no significant edges, check that
        we handle it appropriately.
        '''
        pass

    @mock.patch('api.data_transformations.network_transforms.check_resource_request_validity')
    def test_file_parsing(self, mock_check_resource_request_validity):

        all_resources = Resource.objects.all()
        r1 = all_resources[0]
        r2 = all_resources[1]
        r1.resource_type = MATRIX_KEY
        r1.file_format = TSV_FORMAT
        r1.save()
        r2.resource_type = MATRIX_KEY
        r2.file_format = TSV_FORMAT
        r2.save()
        associate_file_with_resource(r1, self.adj_mtx_fp)
        associate_file_with_resource(r2, self.pval_mtx_fp)

        mock_op_instance = mock.MagicMock()
        mock_op_instance.outputs = {
            'weights_key': 'fake_uuid1',
            'pvals_key': 'fake_uuid2'
        }
        mock_check_resource_request_validity.side_effect = [
            r1,
            r2
        ]
        a, b = get_result_matrices(mock_op_instance, 'weights_key', 'pvals_key')
        self.assertTrue(self.adj_mtx.equals(a))
        self.assertTrue(self.pval_mtx.equals(b))

        # if the wrong key is given, assert that we are given a proper
        # error message
        with self.assertRaisesRegex(Exception, 'BAD_key'):
            get_result_matrices(mock_op_instance, 'BAD_key', 'pvals_key')

        # case matters!
        with self.assertRaisesRegex(Exception, 'Weights_key'):
            get_result_matrices(mock_op_instance, 'Weights_key', 'pvals_key')

    def test_full_call_integration(self):
        '''
        Although we test the individual components of the
        network walking, this uses a demo set of files to ensure
        that everything links up as expected
        '''

        def compare_networks(G, expected_results):
            '''
            Utility method to compare a dictionary representation
            of a network with the networkx.Graph data structure
            '''
            self.assertEqual(len(G.edges()), len(expected_results))
            for key, expected_data in expected_results.items():
                d = G.get_edge_data(key[1], key[0])
                self.assertTrue(np.allclose(d['weight'],expected_data['weight']))
                self.assertTrue(np.allclose(d['pval'],expected_data['pval']))
                self.assertTrue(d['direction'], expected_data['direction'])

        filtered_adj_mtx = filter_by_significance(self.adj_mtx, self.pval_mtx, 0.25)

        # now test each method one by one:
        G = max_edge_subsetting(filtered_adj_mtx, self.pval_mtx, 2, 3)
        expected_results = {
            ('g2', 'g0'): {'weight': 0.740927141, 'pval': 0.0794556750366525, 'direction': POSITIVE_MARKER},
            ('g2', 'm0'): {'weight': 0.030372082, 'pval': 0.2001006992520169, 'direction': POSITIVE_MARKER},
            ('m5', 'm3'): {'weight': 0.331736171, 'pval': 0.2226454235309405, 'direction': POSITIVE_MARKER},
            ('m5', 'm2'): {'weight': 0.297737831, 'pval': 0.2436976205862904, 'direction': NEGATIVE_MARKER},
            ('m3', 'm2'): {'weight': 0.153350004, 'pval': 0.1644257541244821, 'direction': NEGATIVE_MARKER}
        }
        compare_networks(G, expected_results)

        G = max_weight_subsetting(filtered_adj_mtx, self.pval_mtx, 3, 2)
        expected_results = {
            ('g2','g0'): {'weight': 0.740927141, 'pval': 0.0794556750366525, 'direction': POSITIVE_MARKER},
            ('g2','m0'): {'weight': 0.030372082, 'pval': 0.2001006992520169, 'direction': POSITIVE_MARKER},
            ('m5','m3'): {'weight': 0.331736171, 'pval': 0.2226454235309405, 'direction': POSITIVE_MARKER},
            ('m5','m2'): {'weight': 0.297737831, 'pval': 0.2436976205862904, 'direction': NEGATIVE_MARKER},
            ('m3', 'm2'): {'weight': 0.153350004, 'pval': 0.1644257541244821, 'direction': NEGATIVE_MARKER}
        }
        compare_networks(G, expected_results)

        node_list = ['g2', 'm3']
        G = node_list_subsetting(filtered_adj_mtx, self.pval_mtx, node_list, 2)
        expected_results = {
            ('g2','g0'): {'weight': 0.740927141, 'pval': 0.0794556750366525, 'direction': POSITIVE_MARKER},
            ('g2','m0'): {'weight': 0.030372082, 'pval': 0.2001006992520169, 'direction': POSITIVE_MARKER},
            ('m5','m3'): {'weight': 0.331736171, 'pval': 0.2226454235309405, 'direction': POSITIVE_MARKER},
            ('m5','m2'): {'weight': 0.297737831, 'pval': 0.2436976205862904, 'direction': NEGATIVE_MARKER},
            ('m3','m2'): {'weight': 0.153350004, 'pval': 0.1644257541244821, 'direction': NEGATIVE_MARKER}

         }
        compare_networks(G, expected_results)

    @mock.patch('api.data_transformations.network_transforms.walk_for_neighbors')
    def test_max_edge_subsetting(self, mock_walk_for_neighbors):
        filtered_adj_mtx = filter_by_significance(self.adj_mtx, self.pval_mtx, 0.25)
        G = max_edge_subsetting(filtered_adj_mtx, self.pval_mtx, 2, 3)
        args = mock_walk_for_neighbors.call_args[0]
        self.assertTrue(args[0].equals(filtered_adj_mtx))
        self.assertTrue(args[1].equals(self.pval_mtx))
        self.assertCountEqual(args[2], ['g0','g2','m3','m5'])
        self.assertTrue(args[3] == 3)

        # test where we set top_n to zero
        mock_walk_for_neighbors.reset_mock()
        G = max_edge_subsetting(filtered_adj_mtx, self.pval_mtx, 0, 3)
        args = mock_walk_for_neighbors.call_args[0]
        self.assertTrue(args[0].equals(filtered_adj_mtx))
        self.assertTrue(args[1].equals(self.pval_mtx))
        self.assertCountEqual(args[2], [])
        self.assertTrue(args[3] == 3)

        # set very low sig. threshold so nothing is left in filtered_adj_mtx
        filtered_adj_mtx = filter_by_significance(self.adj_mtx, self.pval_mtx, 0.0001)
        mock_walk_for_neighbors.reset_mock()
        G = max_edge_subsetting(filtered_adj_mtx, self.pval_mtx, 2, 3)
        args = mock_walk_for_neighbors.call_args[0]
        self.assertTrue(args[0].equals(filtered_adj_mtx))
        self.assertTrue(args[1].equals(self.pval_mtx))
        self.assertCountEqual(args[2], [])
        self.assertTrue(args[3] == 3)

    @mock.patch('api.data_transformations.network_transforms.walk_for_neighbors')
    def test_max_weight_subsetting(self, mock_walk_for_neighbors):
        filtered_adj_mtx = filter_by_significance(self.adj_mtx, self.pval_mtx, 0.25)
        G = max_weight_subsetting(filtered_adj_mtx, self.pval_mtx, 3, 2)
        args = mock_walk_for_neighbors.call_args[0]
        self.assertTrue(args[0].equals(filtered_adj_mtx))
        self.assertTrue(args[1].equals(self.pval_mtx))
        self.assertCountEqual(args[2], ['g0','g2','m5'])
        self.assertTrue(args[3] == 2)

        # test where we set top_n to zero
        mock_walk_for_neighbors.reset_mock()
        G = max_weight_subsetting(filtered_adj_mtx, self.pval_mtx, 0, 3)
        args = mock_walk_for_neighbors.call_args[0]
        self.assertTrue(args[0].equals(filtered_adj_mtx))
        self.assertTrue(args[1].equals(self.pval_mtx))
        self.assertCountEqual(args[2], [])
        self.assertTrue(args[3] == 3)

        # set very low sig. threshold so nothing is left in filtered_adj_mtx
        filtered_adj_mtx = filter_by_significance(self.adj_mtx, self.pval_mtx, 0.0001)
        mock_walk_for_neighbors.reset_mock()
        G = max_weight_subsetting(filtered_adj_mtx, self.pval_mtx, 2, 3)
        args = mock_walk_for_neighbors.call_args[0]
        self.assertTrue(args[0].equals(filtered_adj_mtx))
        self.assertTrue(args[1].equals(self.pval_mtx))
        self.assertCountEqual(args[2], [])
        self.assertTrue(args[3] == 3)

    @mock.patch('api.data_transformations.network_transforms.walk_for_neighbors')
    def test_node_list_subsetting(self, mock_walk_for_neighbors):
        filtered_adj_mtx = filter_by_significance(self.adj_mtx, self.pval_mtx, 0.25)
        G = node_list_subsetting(filtered_adj_mtx, self.pval_mtx, ['g0', 'm0'], 2)
        args = mock_walk_for_neighbors.call_args[0]
        self.assertTrue(args[0].equals(filtered_adj_mtx))
        self.assertTrue(args[1].equals(self.pval_mtx))
        self.assertCountEqual(args[2], ['g0','m0'])
        self.assertTrue(args[3] == 2)

        mock_walk_for_neighbors.reset_mock()
        G = node_list_subsetting(filtered_adj_mtx, self.pval_mtx, [], 2)
        args = mock_walk_for_neighbors.call_args[0]
        self.assertTrue(args[0].equals(filtered_adj_mtx))
        self.assertTrue(args[1].equals(self.pval_mtx))
        self.assertCountEqual(args[2], [])
        self.assertTrue(args[3] == 2)

        # test where we the node set is not a subset of the available nodes
        mock_walk_for_neighbors.reset_mock()
        with self.assertRaisesRegex(Exception, 'not found'):
            node_list_subsetting(filtered_adj_mtx, self.pval_mtx, ['g0', 'a'], 3)

        filtered_adj_mtx = filter_by_significance(self.adj_mtx, self.pval_mtx, 0.0001)
        self.assertTrue(filtered_adj_mtx.shape[0] == 0)
        mock_walk_for_neighbors.reset_mock()
        G = node_list_subsetting(filtered_adj_mtx, self.pval_mtx, [], 2)
        args = mock_walk_for_neighbors.call_args[0]
        self.assertTrue(args[0].equals(filtered_adj_mtx))
        self.assertTrue(args[1].equals(self.pval_mtx))
        self.assertCountEqual(args[2], [])
        self.assertTrue(args[3] == 2)

        # even though the node is in the matrix, the strict filtering
        # (leaving no sig. edges) leaves us with an empty network and this
        # should fail
        mock_walk_for_neighbors.reset_mock()
        with self.assertRaisesRegex(Exception, 'not found'):
            node_list_subsetting(filtered_adj_mtx, self.pval_mtx, ['g0'], 3)

    def test_add_edges(self):
        adj_mtx = pd.DataFrame(np.array(
            [
                [np.nan, 0.1, -0.2, np.nan],
                [0.1, np.nan, np.nan, np.nan],
                [-0.2, np.nan, np.nan, 0.3],
                [np.nan, np.nan, 0.3, np.nan]
            ]
        ), index=['a','b','c','d'], columns=['a','b','c','d'])
        pval_mtx = pd.DataFrame(np.random.random((4,4)),
                                index=['a','b','c','d'],
                                columns=['a','b','c','d'])
        G = mock.MagicMock()
        G.nodes.return_value = ['a','b','c','d']
        add_edges(G, adj_mtx, pval_mtx)
        G.add_edge.assert_has_calls([
            mock.call('a','b', weight=0.1, pval=pval_mtx.loc['a','b'], direction=POSITIVE_MARKER),
            mock.call('a','c', weight=0.2, pval=pval_mtx.loc['a','c'], direction=NEGATIVE_MARKER),
            mock.call('c','d', weight=0.3, pval=pval_mtx.loc['c','d'], direction=POSITIVE_MARKER)
        ])

        # if no nodes, then no edge add calls:
        G = mock.MagicMock()
        G.nodes.return_value = []
        add_edges(G, adj_mtx, pval_mtx)
        G.add_edge.assert_not_called()

        # if a single node, then no edge add calls:
        G = mock.MagicMock()
        G.nodes.return_value = ['a']
        add_edges(G, adj_mtx, pval_mtx)
        G.add_edge.assert_not_called()
        
        # if the graph only contains two nodes that do not
        # have a 'significant' connection, then no edges:
        G = mock.MagicMock()
        G.nodes.return_value = ['b', 'c']
        add_edges(G, adj_mtx, pval_mtx)
        G.add_edge.assert_not_called()

    def test_add_top_neighbor_nodes(self):
        
        adj_mtx = pd.DataFrame(np.array(
            [
                [np.nan, 0.1, -0.2, np.nan],
                [0.1, np.nan, np.nan, np.nan],
                [-0.2, np.nan, np.nan, 0.3],
                [np.nan, np.nan, 0.3, np.nan]
            ]
        ), index=['a','b','c','d'], columns=['a','b','c','d'])

        # node 'a' has 'b' and 'c' as the two top neighbors
        G = mock.MagicMock()
        row = adj_mtx.loc['a']
        add_top_neighbor_nodes(G, row, 'a', 2)
        G.add_node.assert_has_calls([
            mock.call('b'),
            mock.call('c'),
        ], any_order=True)

        # here, we permit 200 neighbors. there are 
        # only two neighbors that are significantly
        # connected, but that's fine
        G = mock.MagicMock()
        row = adj_mtx.loc['a']
        add_top_neighbor_nodes(G, row, 'a', 200)
        G.add_node.assert_has_calls([
            mock.call('b'),
            mock.call('c'),
        ], any_order=True)

        # only get the top neighbor, which should be 'c'
        G = mock.MagicMock()
        row = adj_mtx.loc['a']
        add_top_neighbor_nodes(G, row, 'a', 1)
        G.add_node.assert_has_calls([
            mock.call('c'),
        ], any_order=True)

        # n=0, so immediately return
        G = mock.MagicMock()
        row = adj_mtx.loc['b']
        add_top_neighbor_nodes(G, row, 'b', 0)
        G.add_node.assert_not_called()

        # n is negative, which doesn't make sense
        G = mock.MagicMock()
        row = adj_mtx.loc['b']
        add_top_neighbor_nodes(G, row, 'b', -1)
        G.add_node.assert_not_called()

    @mock.patch('api.data_transformations.network_transforms.Graph')
    @mock.patch('api.data_transformations.network_transforms.add_top_neighbor_nodes')
    @mock.patch('api.data_transformations.network_transforms.add_edges')
    def test_walk_for_neighbors(self, mock_add_edges, 
                               mock_add_top_neighbor_nodes, mock_graph_class):
        adj_mtx = pd.DataFrame(np.array(
            [
                [np.nan, 0.1, -0.2, np.nan],
                [0.1, np.nan, np.nan, np.nan],
                [-0.2, np.nan, np.nan, 0.3],
                [np.nan, np.nan, 0.3, np.nan]
            ]
        ), index=['a','b','c','d'], columns=['a','b','c','d'])
        pval_mtx = pd.DataFrame(np.random.random((4,4)),
                                index=['a','b','c','d'],
                                columns=['a','b','c','d'])

        mock_graph = mock.MagicMock()
        mock_graph_class.return_value = mock_graph
        root_nodes = ['a', 'b']
        G = walk_for_neighbors(adj_mtx, pval_mtx, root_nodes, 1)
        mock_graph.add_node.assert_has_calls([
            mock.call('a'),
            mock.call('b')
        ])
        # note that since add_top_neighbor_nodes is called 
        # with a pd.Series, we can't use the usual `assert_has_calls`
        # since the pd.Series comparison doesn't work with the '=='
        # operator used by the testing framework
        args0 = mock_add_top_neighbor_nodes.call_args_list[0][0]
        self.assertTrue(args0[0] == mock_graph)
        self.assertTrue(args0[1].equals(adj_mtx.loc['a']))
        self.assertTrue(args0[2] == 'a')
        self.assertTrue(args0[3] == 1)

        args1 = mock_add_top_neighbor_nodes.call_args_list[1][0]
        self.assertTrue(args1[0] == mock_graph)
        self.assertTrue(args1[1].equals(adj_mtx.loc['b']))
        self.assertTrue(args1[2] == 'b')
        self.assertTrue(args1[3] == 1)

        mock_add_edges.assert_called_once()

        mock_add_edges.reset_mock()
        mock_add_top_neighbor_nodes.reset_mock()
        mock_graph = mock.MagicMock()
        mock_graph_class.return_value = mock_graph
        root_nodes = ['a', 'b']
        G = walk_for_neighbors(adj_mtx, pval_mtx, root_nodes, 0)
        # need to add the edges for the root nodes
        mock_add_edges.assert_called_once()
        # zero neighbors were specified, so we do not 
        # search for the top neighbors 
        mock_add_top_neighbor_nodes.assert_not_called()

        mock_add_edges.reset_mock()
        mock_add_top_neighbor_nodes.reset_mock()
        mock_graph = mock.MagicMock()
        mock_graph_class.return_value = mock_graph
        G = walk_for_neighbors(adj_mtx, pval_mtx, [], 1)
        # even though we don't have any nodes added, this
        # function still gets called. It just does not do anything
        mock_add_edges.assert_called_once()
        mock_add_top_neighbor_nodes.assert_not_called()
        mock_graph.add_node.assert_not_called()

    def test_graph_response_format(self):
        '''
        Test that we are formatting our nx.Graph
        object in the desired JSON format
        '''
        G = Graph()
        nodes = ['a','b','c']
        [G.add_node(x) for x in nodes]
        G.add_edge('a', 'b', 
            weight=0.2,
            pval=0.01,
            direction=POSITIVE_MARKER)
        G.add_edge('a', 'c', 
            weight=0.1,
            pval=0.05,
            direction=NEGATIVE_MARKER)
        j = format_response_graph(G)
        expected = {
            'nodes': [
                {'id': 'a'}, 
                {'id': 'b'}, 
                {'id': 'c'}
            ], 
            'edges': [
                {'source': 'a', 'target': 'b', 'weight': 0.2, 'pval': 0.01, 'direction': POSITIVE_MARKER},
                {'source': 'a', 'target': 'c', 'weight': 0.1, 'pval': 0.05, 'direction': NEGATIVE_MARKER}
            ]
        }
        self.assertDictEqual(j, expected)

        G = Graph()
        G.add_node('a')
        j = format_response_graph(G)
        expected = {
            'nodes': [
                {'id': 'a'}
            ], 
            'edges': []
        }
        self.assertDictEqual(j, expected)

        G = Graph()
        j = format_response_graph(G)
        expected = {
            'nodes': [], 
            'edges': []
        }
        self.assertDictEqual(j, expected)

    @mock.patch('api.data_transformations.network_transforms.format_response_graph')
    @mock.patch('api.data_transformations.network_transforms.get_result_matrices')
    @mock.patch('api.data_transformations.network_transforms.filter_by_significance')
    @mock.patch('api.data_transformations.network_transforms.max_edge_subsetting')
    def test_subset_full_network(self, mock_max_edge_subsetting,
        mock_filter_by_significance,
        mock_get_result_matrices,
        mock_format_response_graph):
        '''
        Tests that given the proper params, we make the expected calls
        '''
        mock_adj = mock.MagicMock()
        mock_pvals = mock.MagicMock()
        mock_get_result_matrices.return_value = (
            mock_adj,
            mock_pvals
        )
        mock_filtered_adj_mtx = mock.MagicMock()
        mock_filter_by_significance.return_value =  mock_filtered_adj_mtx

        # first check that we call the proper functions if 
        # the params are all OK
        mock_exec_op = mock.MagicMock()
        query_params = {
            'sig_threshold': 0.2,
            'scheme': 'max_edges',
            'top_n': 2,
            'max_neighbors': 3,
            'weights': 'wk',
            'pvals': 'pvk'
        }
        mock_graph = mock.MagicMock()
        mock_max_edge_subsetting.return_value = mock_graph
        subset_full_network(mock_exec_op, query_params)
        mock_filter_by_significance.assert_called_once_with(mock_adj, mock_pvals, 0.2)
        mock_get_result_matrices.assert_called_once_with(mock_exec_op, 'wk', 'pvk')
        mock_max_edge_subsetting.assert_called_once_with(mock_filtered_adj_mtx, mock_pvals, 2, 3)
        mock_format_response_graph.assert_called_once_with(mock_graph)

    @mock.patch('api.data_transformations.network_transforms.format_response_graph')
    @mock.patch('api.data_transformations.network_transforms.get_result_matrices')
    @mock.patch('api.data_transformations.network_transforms.filter_by_significance')
    @mock.patch('api.data_transformations.network_transforms.max_edge_subsetting')
    @mock.patch('api.data_transformations.network_transforms.max_weight_subsetting')
    @mock.patch('api.data_transformations.network_transforms.node_list_subsetting')
    def test_param_parsing(self, mock_node_list_subsetting,
        mock_max_weight_subsetting,
        mock_max_edge_subsetting,
        mock_filter_by_significance,
        mock_get_result_matrices,
        mock_format_response_graph):
        '''
        Tests that we handle all the params as expected. These
        are passed by get params to the api and passed to
        the function
        '''

        # missing weights and pvals keys-- these are
        # the files we need to pull data from, so 
        # they are necessary
        mock_exec_op = mock.MagicMock()
        query_params = {
            'sig_threshold': 0.2,
            'scheme': 'max_edges',
            'top_n': 2,
            'max_neighbors': 3,
            'pvals': 'pvk'
        }
        with self.assertRaisesRegex(Exception, 'weights'):
            subset_full_network(mock_exec_op, query_params)

        query_params = {
            'sig_threshold': 0.2,
            'scheme': 'max_edges',
            'top_n': 2,
            'max_neighbors': 3,
            'weights': 'wt'
        }
        with self.assertRaisesRegex(Exception, 'pvals'):
            subset_full_network(mock_exec_op, query_params)

        # test missing scheme
        query_params = {
            'sig_threshold': 0.2,
            'top_n': 2,
            'max_neighbors': 3,
            'weights': 'wt',
            'pvals': 'pvk'
        }
        with self.assertRaisesRegex(Exception, 'scheme'):
            subset_full_network(mock_exec_op, query_params)

        # test incorrect scheme
        query_params = {
            'sig_threshold': 0.2,
            'top_n': 2,
            'scheme': 'JUNK',
            'max_neighbors': 3,
            'weights': 'wt',
            'pvals': 'pvk'
        }
        with self.assertRaisesRegex(Exception, 'not an available option'):
            subset_full_network(mock_exec_op, query_params)

        # test that we require a significance value (float [0,1])
        query_params = {
            'sig_threshold': 1.2,
            'top_n': 2,
            'scheme': 'max_edges',
            'max_neighbors': 3,
            'weights': 'wt',
            'pvals': 'pvk'
        }
        with self.assertRaisesRegex(Exception, 'not within the bounds'):
            subset_full_network(mock_exec_op, query_params)

        # sig threshold not a number
        query_params = {
            'sig_threshold': 'a',
            'top_n': 2,
            'scheme': 'max_edges',
            'max_neighbors': 3,
            'weights': 'wt',
            'pvals': 'pvk'
        }
        with self.assertRaisesRegex(Exception, 'could not be parsed as a number'):
            subset_full_network(mock_exec_op, query_params)

        # test max_neighbors param that is not numeric
        query_params = {
            'sig_threshold': '0.5', # this is OK
            'top_n': 2,
            'scheme': 'max_edges',
            'max_neighbors': 'a',
            'weights': 'wt',
            'pvals': 'pvk'
        }
        with self.assertRaisesRegex(Exception, 'could not be parsed as a positive integer'):
            subset_full_network(mock_exec_op, query_params)

        # although passing a numeric 1.2 is fine (since int(...) cast works),
        # having int(...) parse a stringified float is not acceptable.
        query_params = {
            'sig_threshold': 0.05,
            'top_n': 2,
            'scheme': 'max_edges',
            'max_neighbors': '1.2',
            'weights': 'wt',
            'pvals': 'pvk'
        }
        with self.assertRaisesRegex(Exception, 'could not be parsed as a positive integer'):
            subset_full_network(mock_exec_op, query_params)
        
        # test max_neighbors param that is negative
        query_params = {
            'sig_threshold': 0.05,
            'top_n': 2,
            'scheme': 'max_edges',
            'max_neighbors': -3,
            'weights': 'wt',
            'pvals': 'pvk'
        }
        with self.assertRaisesRegex(Exception, '-3 was not a positive integer'):
            subset_full_network(mock_exec_op, query_params)

        # if the scheme is either by edge or weight priority, check that
        # we require top_n
        query_params = {
            'sig_threshold': 0.05,
            'scheme': 'max_edges',
            'max_neighbors': 2,
            'weights': 'wt',
            'pvals': 'pvk'
        }
        with self.assertRaisesRegex(Exception, 'must supply a "\'top_n\'" parameter'):
            subset_full_network(mock_exec_op, query_params)

        # top_n needs to be a positive int. Zero should fail
        query_params = {
            'sig_threshold': 0.05,
            'scheme': 'max_edges',
            'top_n': 0,
            'max_neighbors': 2,
            'weights': 'wt',
            'pvals': 'pvk'
        }
        with self.assertRaisesRegex(Exception, 'was not a positive integer'):
            subset_full_network(mock_exec_op, query_params)

        # top_n is a positive int
        query_params = {
            'sig_threshold': 0.05,
            'scheme': 'max_edges',
            'top_n': -1,
            'max_neighbors': 2,
            'weights': 'wt',
            'pvals': 'pvk'
        }
        with self.assertRaisesRegex(Exception, 'was not a positive integer'):
            subset_full_network(mock_exec_op, query_params)

        # if the scheme is 'node_list', we need a set of starting nodes
        # supplied with the 'nodes' param
        query_params = {
            'sig_threshold': 0.05,
            'scheme': 'node_list',
            'max_neighbors': 2,
            'weights': 'wt',
            'pvals': 'pvk'
        }
        with self.assertRaisesRegex(Exception, 'must supply a "\'nodes\'" parameter'):
            subset_full_network(mock_exec_op, query_params)

    @mock.patch('api.data_transformations.network_transforms.format_response_graph')
    @mock.patch('api.data_transformations.network_transforms.get_result_matrices')
    @mock.patch('api.data_transformations.network_transforms.filter_by_significance')
    @mock.patch('api.data_transformations.network_transforms.max_edge_subsetting')
    def test_missing_max_neighbors_default(self, mock_max_edge_subsetting,
        mock_filter_by_significance,
        mock_get_result_matrices,
        mock_format_response_graph):
        '''
        If max_neighbors is not supplied as an input, check that it's
        set to zero and we only get the 'top-level' nodes
        '''
        mock_adj = mock.MagicMock()
        mock_pvals = mock.MagicMock()
        mock_get_result_matrices.return_value = (
            mock_adj,
            mock_pvals
        )
        mock_filtered_adj_mtx = mock.MagicMock()
        mock_filter_by_significance.return_value =  mock_filtered_adj_mtx

        # first check that we call the proper functions if 
        # the params are all OK
        mock_exec_op = mock.MagicMock()
        query_params = {
            'sig_threshold': 0.2,
            'scheme': 'max_edges',
            'top_n': 2,
            'weights': 'wk',
            'pvals': 'pvk'
        }
        mock_graph = mock.MagicMock()
        mock_max_edge_subsetting.return_value = mock_graph
        subset_full_network(mock_exec_op, query_params)
        mock_filter_by_significance.assert_called_once_with(mock_adj, mock_pvals, 0.2)
        mock_get_result_matrices.assert_called_once_with(mock_exec_op, 'wk', 'pvk')
        mock_max_edge_subsetting.assert_called_once_with(mock_filtered_adj_mtx, mock_pvals, 2, 0)
        mock_format_response_graph.assert_called_once_with(mock_graph)

    @mock.patch('api.data_transformations.network_transforms.format_response_graph')
    @mock.patch('api.data_transformations.network_transforms.get_result_matrices')
    @mock.patch('api.data_transformations.network_transforms.filter_by_significance')
    @mock.patch('api.data_transformations.network_transforms.node_list_subsetting')
    def test_missing_max_neighbors_default(self, mock_node_list_subsetting,
        mock_filter_by_significance,
        mock_get_result_matrices,
        mock_format_response_graph):
        '''
        Tests that we properly handle the case where a user gives a
        list of node names
        '''
        mock_adj = mock.MagicMock()
        mock_pvals = mock.MagicMock()
        mock_get_result_matrices.return_value = (
            mock_adj,
            mock_pvals
        )
        mock_filtered_adj_mtx = mock.MagicMock()
        mock_filter_by_significance.return_value =  mock_filtered_adj_mtx

        # first check that we call the proper functions if 
        # the params are all OK
        mock_exec_op = mock.MagicMock()
        query_params = {
            'sig_threshold': 0.2,
            'scheme': 'node_list',
            'max_neighbors': 2,
            'nodes': 'a,b,c',
            'weights': 'wk',
            'pvals': 'pvk'
        }
        mock_graph = mock.MagicMock()
        mock_node_list_subsetting.return_value = mock_graph
        subset_full_network(mock_exec_op, query_params)
        mock_filter_by_significance.assert_called_once_with(mock_adj, mock_pvals, 0.2)
        mock_get_result_matrices.assert_called_once_with(mock_exec_op, 'wk', 'pvk')
        mock_node_list_subsetting.assert_called_once_with(mock_filtered_adj_mtx,
            mock_pvals, ['a','b','c'], 2)
        mock_format_response_graph.assert_called_once_with(mock_graph)

    @mock.patch('api.data_transformations.network_transforms.get_result_matrices')
    def test_empty(self, mock_get_result_matrices):
        '''
        If filtered matrix is empty (no sig. edges at the chosen threshold),
        check that we return empty nodes/edges
        '''
        mock_get_result_matrices.return_value = (
            self.adj_mtx,
            self.pval_mtx
        )

        # first check that we call the proper functions if 
        # the params are all OK
        mock_exec_op = mock.MagicMock()
        query_params = {
            'sig_threshold': 0.00001,
            'scheme': 'max_edges',
            'top_n': 2,
            'weights': 'wk',
            'pvals': 'pvk'
        }
        j = subset_full_network(mock_exec_op, query_params)
        expected = {
            'nodes': [], 
            'edges': []
        }
        self.assertDictEqual(j, expected)