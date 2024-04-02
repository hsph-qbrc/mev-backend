import unittest.mock as mock
import os
import shutil
import json

from api.tests.base import BaseAPITestCase

from api.utilities.nextflow_utils import get_container_names, \
    extract_process_containers, \
    edit_nf_containers, \
    get_nextflow_file_contents, \
    job_succeeded, \
    get_error_report

# the api/tests dir
TESTDIR = os.path.dirname(__file__)
DEMO_TESTDIR = os.path.join(TESTDIR, 'operation_test_files', 'demo_nextflow_workflow')


class NextflowUtilsTester(BaseAPITestCase):

    def test_extracts_container_ids(self):
        '''
        testing the method which scans the nextflow script
        text and returns the containers used.
        '''
        nf_text = open(os.path.join(DEMO_TESTDIR, 'main.nf')).read()
        container_set = extract_process_containers(nf_text)
        expected_set = set(['ghcr.io/web-mev/mev-hcl', \
            'ghcr.io/web-mev/pca:sha-ede315244ea21f91be287b9504dbb71bc9ee3f2e', \
            'docker.io/ubuntu:jammy'
        ])
        self.assertTrue(container_set == expected_set)

    @mock.patch('api.utilities.nextflow_utils.get_nextflow_file_contents')
    @mock.patch('api.utilities.nextflow_utils.extract_process_containers')
    def test_extract_full_image_set(self, \
            mock_extract_process_containers, \
            mock_get_nextflow_file_contents):
        '''
        Tests the function that reads the nextflow script files
        and returns the associated containers.
        '''

        mock_get_nextflow_file_contents.return_value = {'f1':'abc','f2':'xyz'}
        mock_extract_process_containers.side_effect = [
            set(['a','b']),
            set(['a','c'])
        ]

        final_set = get_container_names('')
        self.assertCountEqual(final_set, ['a','b','c'])
        mock_extract_process_containers.assert_has_calls([
            mock.call('abc'),
            mock.call('xyz')
        ])


    def test_get_nextflow_file_contents(self):
        result = get_nextflow_file_contents(DEMO_TESTDIR)
        contents = open(os.path.join(DEMO_TESTDIR, 'main.nf')).read()
        # in the results dict, the keys are the filenames
        self.assertCountEqual(result.keys(), ['main.nf'])
        self.assertTrue(result['main.nf'] == contents)


    def test_container_edits(self):
        '''
        Tests that we add the proper tags to the container
        '''
        tmpdir = os.path.join('/tmp', 'test_nf')
        shutil.copytree(DEMO_TESTDIR, tmpdir)
        # this mapping has the original name (parsed from the nf file)
        # mapped to the final image name. Note that this test doesn't perform
        # the logic of asserting those names are appropriate, so we can map
        # to anything for the purpose of this test.
        name_mapping = {
            'ghcr.io/web-mev/pca:sha-ede315244ea21f91be287b9504dbb71bc9ee3f2e':'pca_container',
            'ghcr.io/web-mev/mev-hcl':'hcl_container',
            'docker.io/ubuntu:jammy':'ubuntu_container'
        }

        edit_nf_containers(tmpdir, name_mapping)

        # now parse the docker images in that directory to confirm they were changed:
        images = get_container_names(tmpdir)
        expected_set = set([
            'pca_container',
            'hcl_container',
            'ubuntu_container'
        ])        
        self.assertTrue(set(images) == expected_set)
        shutil.rmtree(tmpdir)

    def test_nextflow_job_succeeded_parse(self):
        j = json.load(open(os.path.join(TESTDIR, 'nextflow_job_complete_metadata.json')))
        self.assertTrue(job_succeeded(j))

    def test_returns_error_report(self):
        '''
        If a nextflow-based job fails, the report metadata will have a trace that
        we can save/show. Test that the function works as expected
        '''
        # errorReport has a long message which includes a lot of nextflow-related "fluff"
        # errorMessage has a more condensed stderr/stdout from the process itself.
        job_metadata = {
            'metadata': {
                'workflow': {
                    'something': 1,
                    'errorReport': 'some error',
                    'errorMessage': 'some error'
                }
            }
        }
        r = get_error_report(job_metadata)
        self.assertTrue('some error' in r)

        # test that we catch the edge case where the job metadata
        # is missing that key for whatever reason:
        job_metadata = {
            'metadata': {
                'workflow': {
                    'something': 1,
                }
            }
        }
        r = get_error_report(job_metadata)
        self.assertTrue('Unexpected error' in r)