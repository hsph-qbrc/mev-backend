import os
import sys
import logging
import inspect
import requests

from django.core.management.base import BaseCommand
from django.conf import settings

from api.utilities.basic_utils import run_shell_command, \
    make_local_directory, \
    copy_local_resource
from api.models import PublicDataset
from api.public_data import check_if_valid_public_dataset_name, \
    prepare_dataset

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = ('Creates some stubbed out modules and other items required'
        ' for the creation of a new public dataset and Solr core'
    )

    def add_arguments(self, parser):

        # argument to control whether we push to github.  Note that it is
        # "paired" with the one below to create a more conventional "switch flag"
        parser.add_argument(
            '-d',
            '--dataset_id',
            required=True,
            help=('The unique identifier of the public dataset to prepare. This will'
                ' be the name of the Solr core as well.')
        )

        parser.add_argument(
            '-f',
            '--example_file',
            required=True,
            help=('An example (even a row-subset) of the data you want to index'
                ' into this core. Must be an accurate representation of the data'
                ' since we do not allow new fields to be dynamically added to the'
                ' schema once it has been initialized.'
            )
        )

    def create_class_stub(self, dataset_id):
        '''
        Creates a stub module to save a bit of work. Note that we simply
        place the module in the root of the api/public_data/sources dir
        and one may wish to move that around (like with the GDC/TCGA datasets)
        to create a hierarchy.

        Also note that we DO NOT edit the `IMPLEMENTING_CLASSES` list in 
        api/public_data/__init__.py which is necessary to actually "register"
        this new dataset
        '''

        module_template = '''
        from api.public_data.sources.base import PublicDataSource

        class {camelizedTag}Dataset(PublicDataSource):

            TAG = '{tag}'
            PUBLIC_NAME = ''
            DESCRIPTION = ''
            DATASET_FILES = []

            def prepare(self):
                pass

            def verify_files(self, file_dict):
                # verifies that all required files are present
                pass

            def get_indexable_files(self, file_dict):
                # Returns a list of files that we should index given
                # the dictionary. Some files do not get indexed, but
                # are necessary for a particular dataset
                return []

            def get_additional_metadata(self):
                # Returns a dict which contains additional dataset-
                # specific information
                return {}

            def create_from_query(self, query_params):
                # subsets the dataset based on the query_params.
                # Returns a tuple of a filepath (string) and
                # a resource_type
                pass
        '''
        # turns 'abc' to 'Abc' so it looks more like a canonical class name
        camelizedTag = dataset_id.title().replace('-', '').replace('_', '')
        module_str = module_template.format(
            camelizedTag = camelizedTag,
            tag = dataset_id
        )
        module_path = os.path.join(
            settings.BASE_DIR, 
            'api',
            'public_data',
            'sources',
            dataset_id + '.py'
        )
        with open(module_path, 'w') as fout:
            fout.write(inspect.cleandoc(module_str) + '\n')
        logger.info('\n\nWrote the new stubbed out module to {p}. EDIT/MOVE AS NECESSARY!'
            ' \n\nAlso note that you still need to add this to the IMPLEMENTING_CLASSES'
            ' list in mev/api/public_data/__init__.py before it can be used.'.format(
            p = module_path
        ))

    def create_new_core(self, dataset_id):
        '''
        Executes the Solr create_core command.
        '''
        logger.info('\n\nAttempt to create a Solr core.')
        cmd = 'sudo -u solr /opt/solr/bin/solr create_core -c {d}'.format(d=dataset_id)
        run_shell_command(cmd)

    def index_example_file(self, dataset_id, filepath):
        '''
        Indexes a file into the core
        '''
        cmd = '/opt/solr/bin/post -c {d} {f}'.format(
            d = dataset_id,
            f = filepath
        )
        run_shell_command(cmd)

    def query_for_autoschema(self, dataset_id, core_dir):
        '''
        Issues a GET request to the solr server to get the
        autogenerated schema created by indexing our example file
        This will ultimately have to manually edited to ensure the 
        auto-schema is reasonable for the provided dataset.
        '''
        u = 'http://localhost:8983/solr/{d}/schema?wt=schema.xml'.format(
            d = dataset_id
        )
        r = requests.get(u)
        if r.status_code != 200:
            raise Exception('Failed to query the core for the autogenerated schema.xml.')
        
        schema_path = os.path.join(
            core_dir,
            'auto_schema.{d}.xml'.format(d=dataset_id)
        )
        with open(schema_path, 'w') as fout:
            fout.write(r.text)
        logger.info('\n\nWrote the auto-generated schema.xml to: {p}.'
            ' Please open and edit to ensure it matches your'
            ' expectations of the dataset.\n\nAlso note that'
            ' you will need to rename it to schema.xml for Solr to find it.'.format(
                p = schema_path
            )
        )

    def setup_files(self, dataset_id):
        '''
        This creates the solr core directory for the repository 
        and adds the proper files
        '''
        solr_home_path = os.path.join(
            os.path.dirname(settings.BASE_DIR),
            'solr'
        )
        core_dir_path = os.path.join(
            solr_home_path,
            dataset_id
        )
        make_local_directory(core_dir_path)

        # copy the solrconfig template. This solrconfig prevents
        # certain default behavior which could cause problems. Examples
        # include adding new fields to existing schema-- we don't want that
        basic_config_path = os.path.join(
            solr_home_path, 
            'basic_solrconfig.xml'
        )
        dest = os.path.join(
            core_dir_path,
            'solrconfig.xml'
        )
        copy_local_resource(basic_config_path, dest)

        return core_dir_path

    def handle(self, *args, **options):

        dataset_id = options['dataset_id']
        filepath = options['example_file']

        self.create_class_stub(dataset_id)

        try:
            self.create_new_core(dataset_id)
            logger.info('\n\nCore created!')
        except Exception as ex:
            logger.info('Failed to create core. Reason was: {s}. Exiting.'.format(s=ex))
            sys.exit(1)

        
        try:
            self.index_example_file(dataset_id, filepath)
            logger.info('\n\nFile indexed!')
        except Exception as ex:
            logger.info('Failed to index the file at {f}.'
                ' Reason was: {s}. Exiting.'.format(
                    f = filepath,
                    s=ex
                )
            )
            sys.exit(1)

        core_dir = self.setup_files(dataset_id)
        self.query_for_autoschema(dataset_id, core_dir)

        puppet_snippet = '''
          solr::core {{ '{d}':
            schema_src_file     => "${{project_root}}/solr/{d}/schema.xml",
            solrconfig_src_file => "${{project_root}}/solr/{d}/solrconfig.xml",
          }}
        '''.format(d=dataset_id)
        
        logger.info('\n\n\nDo not forget to add this core to your '
            ' Puppet manifest at deploy/puppet/mevapi/manifests/init.pp.'
            ' For instance: \n{s}\n\nWe do NOT do this automagically!'.format(
                s=puppet_snippet
            )
        )

        logger.info('\n\nWhen you are fully satisfied with this new core, be sure'
            ' to add the files in {d} to your git repository.'.format(d=core_dir)
        )
