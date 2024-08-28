import os
import requests
import json
import logging
import mimetypes

from api.utilities.basic_utils import read_local_file
from api.public_data.indexers.base import BaseIndexer

logger = logging.getLogger(__name__)


class SolrIndexer(BaseIndexer):
    """
    This class implements our interface to the solr service
    """
    # TODO: extract this to settings or otherwise
    SOLR_BIN_DIR = '/opt/solr/bin'

    SOLR_POST_CMD = '{bin_dir}/post'.format(bin_dir=SOLR_BIN_DIR)
    SOLR_CMD = '{bin_dir}/solr'.format(bin_dir=SOLR_BIN_DIR)

    # TODO: extract this to settings or otherwise
    SOLR_SERVER = 'http://localhost:8983/solr'

    # relative to the SOLR_SERVER url
    CORES_URL = 'solr/admin/cores'

    def index(self, core_name, filepath):
        '''
        Indexes/commits a file by a POST request
        Note that the file is committed automatically, so it's
        not an all-or-nothing if the calling function is attempting
        to index a bunch of documents.
        '''
        logger.info('Using solr to index the following file: {f}'.format(f=filepath))

        # Check if the core exists. If not, we exit
        if not self._check_if_core_exists(core_name):
            logger.info('The core ({idx}) must already exist.'.format(
                idx = core_name
            ))
            return

        content_type, encoding = mimetypes.guess_type(filepath)
        logger.info('Inferred the mime-type of {f} to be: {m}'.format(
            f = filepath,
            m = content_type
        ))

        with read_local_file(filepath) as fin:
            data = fin.read()

        headers = {'content-type': content_type}
        params = {'commit': 'true'}
        u = '{host}/{core}/update/'.format(host=self.SOLR_SERVER, core=core_name)
        r = requests.post(u, data=data, params=params, headers=headers)
        if r.status_code == 200:
            logger.info('Successfully indexed and committed {f}'.format(f=filepath))
        else:
            logger.info('Failed to index or commit {f}.'
                ' The response was: {j}'.format(
                    f = filepath,
                    j = r.json()
                )
            )
            raise Exception('Failed to index/commit {f}'.format(f=filepath))

    def query(self, index_name, query_string):
        '''
        Perform and return the query response from solr.
        `index_name` identifies the solr collection/core we are querying
        `query_string` is the query string appended to the url.
        '''
        query_url = '{base_url}/{index_name}/select?{query_str}'.format(
            base_url = self.SOLR_SERVER,
            index_name = index_name,
            query_str = query_string
        ) 
        r = requests.get(query_url)
        if r.status_code == 200:
            try:
                j = r.json()
            except json.decoder.JSONDecodeError:
                logger.error('The response had status 200, but'
                    ' could not be parsed as JSON.'
                )
                raise Exception('Unexpected response from solr server.'
                    ' The query was successful, but could not interpret'
                    ' the response as JSON.'
                )
            return self._reformat_response(j) 
        else:
            payload = r.json()
            error_msg = payload['error']['msg']
            logger.info('The query to {u} failed. Message was: {m}'.format(
                u=query_url,
                m = error_msg
            ))
            raise Exception('Query failed. Error message was: {m}'.format(
                m = error_msg
            ))

    def _reformat_response(self, solr_response):
        '''
        This method reformats the response provided by solr. Depending on how
        the query was presented, we want to return different formats to the front
        end. 
        '''
        return solr_response

    def _check_if_core_exists(self, index_name):
        '''
        Return a boolean indicating whether there is already
        a core with the given name
        '''
        # A reliable way to check if a core exists is to attempt a query on it.
        # If the core does NOT exist, should get a 404 response

        # Here, make a wildcard query for a single record. 
        url = '{base_url}/{index_name}/select?{query_str}'.format(
            base_url = self.SOLR_SERVER,
            index_name = index_name,
            query_str = 'q=*:*&rows=1'
        ) 
        r = requests.get(url)
        if r.status_code == 200:
            return True
        return False


    
