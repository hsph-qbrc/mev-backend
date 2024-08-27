import json
import os
import uuid
import logging
import subprocess as sp
import shutil
import requests

from django.conf import settings

from exceptions import DataStructureValidationException, \
    InvalidRunModeException
from constants import RESOURCE_TYPE_SET

from data_structures.data_resource_attributes import get_all_data_resource_typenames, \
    OperationDataResourceAttribute
from data_structures.operation import Operation

from api.models import Operation as OperationDbModel
from api.utilities.basic_utils import recursive_copy
from api.utilities.operations import read_operation_json
from api.runners import get_runner, AVAILABLE_RUNNERS


logger = logging.getLogger(__name__)


def add_required_keys_to_operation(op_dict, **kwargs):
    '''
    When an analysis developer creates an Operation suitable for MEV, they
    do not have to specify keys like `id`, which is a unique UUID only used
    internally. However, they are necessary to create a properly
    functioning `Operation` instance.
    This function checks for those keys and adds them.
    '''
    op_dict.update(kwargs)


def retrieve_commit_hash(git_dir):
    '''
    Retrieves the git commit ID given a directory
    '''
    logger.info('Retrieve commit ID.')
    cmd = 'git show -s --format=%H'
    logger.info('Retrieve git commit with: {cmd}'.format(
        cmd=cmd
    ))
    cmd = cmd.split(' ')

    p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, cwd=git_dir)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        logger.error('Problem with querying the'
                     ' commit hash from the git repo at {git_dir}.\n'
                     'STDERR was: {stderr}\nSTDOUT was: {stdout}'.format(
                         git_dir=git_dir,
                         stderr=stderr,
                         stdout=stdout
                     )
                     )
        raise Exception('Failed when querying the git commit ID. See logs.')
    else:
        commit_hash = stdout.strip().decode('utf-8')
        return commit_hash


def checkout_branch(git_dir, commit_id):
    '''
    Changes given a given git directory to the desired commit
    '''
    logger.info('Attempt to checkout commit {commit_id}'.format(
        commit_id=commit_id))
    cmd = 'git checkout {commit_id}'.format(
        git_dir=git_dir,
        commit_id=commit_id
    )
    logger.info('Checkout commit with: {cmd}'.format(
        cmd=cmd
    ))
    cmd = cmd.split(' ')

    p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, cwd=git_dir)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        logger.error('Problem with checking out'
                     ' commit {commit_id} from the git repo at {git_dir}.\n'
                     'STDERR was: {stderr}\nSTDOUT was: {stdout}'.format(
                         commit_id=commit_id,
                         git_dir=git_dir,
                         stderr=stderr,
                         stdout=stdout
                     )
                     )
        raise Exception(
            'Failed when attemping to checkout a particular commit. See logs.')
    else:
        commit_hash = stdout.strip().decode('utf-8')
        return commit_hash


def retrieve_repo_name(git_dir):
    '''
    Retrieves the git repository name given a directory
    '''
    logger.info('Retrieve git repo name')
    cmd = 'git remote get-url origin'.format(
        git_dir=git_dir
    )
    logger.info('Retrieve git repo name with: {cmd}'.format(
        cmd=cmd
    ))
    cmd = cmd.split(' ')

    p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, cwd=git_dir)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        logger.error('Problem with querying the'
                     ' repo name from the git repo at {git_dir}.\n'
                     'STDERR was: {stderr}\nSTDOUT was: {stdout}'.format(
                         git_dir=git_dir,
                         stderr=stderr,
                         stdout=stdout
                     )
                     )
        raise Exception('Failed when querying the git commit ID. See logs.')
    else:
        git_str = stdout.strip().decode('utf-8')
        logger.info('Repo was found to be: {x}'.format(x=git_str))
        final_piece = git_str.split('/')[-1]
        if final_piece.endswith('.git'):
            return final_piece[:-4]
        else:
            return final_piece


def clone_repository(url):
    '''
    This clones the repository and returns the destination dir
    '''
    uuid_str = str(uuid.uuid4())
    dest = os.path.join(settings.CLONE_STAGING_DIR, uuid_str)
    clone_cmd = 'git clone %s %s' % (url, dest)
    clone_cmd = clone_cmd.split(' ')
    logger.info('About to clone repository with command: {cmd}'.format(
        cmd=clone_cmd
    ))
    p = sp.Popen(clone_cmd, stdout=sp.PIPE, stderr=sp.STDOUT)
    stdout, stderr = p.communicate()

    if p.returncode != 0:
        logger.error('Problem when cloning the repository.\n'
                     ' STDERR was: {stderr}\n'
                     ' STDOUT was: {stdout}'.format(
                         stderr=stderr,
                         stdout=stdout
                     )
                     )
        raise Exception('Failed when cloning the repository. See logs.')
    logger.info('Completed clone.')
    return dest


def check_required_files(op_data, staging_dir):
    '''
    Depending on how an Operation is run (local, cromwell), we have different
    requirements for the files needed.
    '''
    run_mode = op_data['mode']
    runner_class = get_runner(run_mode)
    runner = runner_class()
    runner.check_required_files(staging_dir)


def handle_operation_specific_resources(op, staging_dir, op_uuid):
    '''
    This function looks through the operation's inputs and handles any 
    operation-specific resources. These are user-independent resources (such as 
    genome indices) that are associated with an Operation, but are obviously 
    distinct from a user's files.
    '''

    # look through the inputs to see if any are OperationDataResource type.
    # If not, immediately return
    op_inputs = op.inputs
    for key in op_inputs.keys():
        op_input = op_inputs[key]
        if op_input.is_data_resource_input() \
            and (not op_input.is_user_data_resource_input()):
            # TODO: implement when ready.
            raise NotImplementedError('To use/incorporate operation resources,'
                                  ' you must first create the logic.')


def prepare_operation(op_data, operation_db_obj, staging_dir, repo_name, git_hash):
    '''
    This function calls out to the runner to have it prepare the necessary
    elements to run the Operation.

    For instance, in a local Docker-based job, we need to pull the container
    For a cromwell job, we need to check the containers and push to dockerhub
    '''
    run_mode = op_data['mode']
    runner_class = get_runner(run_mode)
    runner = runner_class()
    runner.prepare_operation(operation_db_obj, staging_dir, repo_name, git_hash)


def check_for_repo(repository_url):
    '''
    This function checks that we can reach a particular repository.

    If a bad url is given, the GET request will return 404. This is different
    than the behavior with a `git clone` where a bad repo url will first
    attempt to log you in via the terminal (which requires interactivity or
    storing github keys). Recall that we will only work with public repos
    anyway.
    '''
    r = requests.get(repository_url)
    if r.status_code == 200:
        return
    else:
        raise Exception('Could not find the repository'
                        ' at {r} or it was not public'.format(r=repository_url))


def perform_operation_ingestion(repository_url, op_uuid, commit_id, overwrite=False):
    '''
    This function is the main entrypoint for the ingestion of a new `Operation`
    '''

    # Check that we can find this
    check_for_repo(repository_url)

    # pull from the repository:
    staging_dir = clone_repository(repository_url)

    if commit_id:
        # if provided with a commit ID, check that out
        checkout_branch(staging_dir, commit_id)
        git_hash = commit_id
    else:
        git_hash = retrieve_commit_hash(staging_dir)

    repo_name = retrieve_repo_name(staging_dir)
    try:
        ingest_dir(staging_dir,
            op_uuid,
            git_hash,
            repo_name,
            repository_url,
            overwrite=overwrite)
    except Exception as ex:
        logger.info('Failed to ingest directory. See logs.'
                    ' Exception was: {ex}'.format(ex=ex)
                    )
        raise ex
    finally:
        # remove the staging dir:
        shutil.rmtree(staging_dir)

def check_single_input_or_output(
    current_input_output, data_resource_typenames):
    '''
    Given a data_structures.operation_input.OperationInput (or the equivalent
    output version), check that it conforms to the expectations of WebMeV.
    For an example of what that all means, see the docstring for
    `validate_operation_spec`
    '''
    spec = current_input_output.spec.value
    if spec.typename in data_resource_typenames:
        spec.check_resource_type_keys(RESOURCE_TYPE_SET)


def validate_operation_spec(op):
    '''
    This function verifies that the operation specification is valid
    beyond its basic structure.

    The `op` argument is an instance of data_structures.operation.Operation.

    To avoid placing Operation logic in multiple places, the 
    data_structures.operation.Operation class only checks the structure
    of the operation specification. It does not perform any logic 
    specific to the API. We check/validate that here.

    As an example, consider an Operation that takes a DataResource
    attribute. The data_structures.operation.Operation class checks 
    that it contains a "resource_type" key, but does not check that the
    value is appropriate/valid (e.g. "MTX" for a numeric matrix type).
    '''
    # this gets a list of the inputs/outputs that concern files 
    # ("data resources") and hence those that need to be checked
    # for validity (e.g. that the `resource_type` field has a valid
    # string value for a DataResourceAttribute)
    data_resource_typenames = get_all_data_resource_typenames()

    # check the inputs/outputs for data resources (files) and
    # verify that the resource_types given are actually valid.
    for k in op.inputs.keys():
        current_input = op.inputs[k]
        check_single_input_or_output(current_input, data_resource_typenames)
    for k in op.outputs.keys():
        current_output = op.outputs[k]
        check_single_input_or_output(current_output, data_resource_typenames)
        
    # Check that the run mode is valid for our available job runnres.
    if not op.mode in AVAILABLE_RUNNERS:
        raise InvalidRunModeException('The operation specification'
            f' provided a run mode "{op.mode}" which was not known. Options '
            f' are: {",".join(AVAILABLE_RUNNERS)}')


def ingest_dir(staging_dir, op_uuid, git_hash, repo_name, repository_url, overwrite=False):

    # Parse the JSON file defining this new Operation:
    operation_json_filepath = os.path.join(
        staging_dir, settings.OPERATION_SPEC_FILENAME)
    j = read_operation_json(operation_json_filepath)

    # extra parameters for an Operation that are not required
    # to be specified by the developer who wrote the `Operation`
    add_required_keys_to_operation(j, id=op_uuid,
                                   git_hash=git_hash,
                                   repository_url=repository_url,
                                   repository_name=repo_name
                                   )

    # attempt to validate the data for the operation. Note that the
    # constructor for data_structures.operation.Operation will validate
    # the structure, but not the content. We check that below.
    try:
        op = Operation(j)
    except DataStructureValidationException as ex:
        logger.info('A formatting error was raised when validating'
                    ' the information parsed from the operation spec file'
                    f' located at {operation_json_filepath}. Exception was: {ex}.')
        raise ex
    except Exception as ex:
        logger.info('An unexpected error was raised when validating'
                    ' the information parsed from the operation spec file'
                    f' located at {operation_json_filepath}. Exception was: {ex}.')
        raise ex

    validate_operation_spec(op)

    # Get the dict representation of the Operation (data structure, not database model)
    op_data = op.to_dict()
    logging.info(f'After parsing operation spec, we have: {op_data}')

    # check that the required files, etc. are there for the particular run mode:
    check_required_files(op_data, staging_dir)

    # handle any operation-specific resources/files:
    handle_operation_specific_resources(op, staging_dir, op_uuid)

    # prepare any elements required for running the operation:
    op_db_obj = OperationDbModel.objects.get(id=op.id)
    prepare_operation(op_data, op_db_obj, staging_dir, repo_name, git_hash)

    # save the operation in a final location:
    save_operation(op_data, staging_dir, overwrite)

    # update the database instance.
    try:
        o = OperationDbModel.objects.get(id=op.id)
        o.name = op.name
        o.active = True
        o.successful_ingestion = True
        o.workspace_operation = op_data['workspace_operation']
        o.git_commit = git_hash
        o.repository_url = repository_url
        o.save()
    except OperationDbModel.DoesNotExist:
        logger.error('Could not find the Operation corresponding to'
                     f' id={op_uuid}')
        raise Exception('Encountered issue when trying update an Operation'
                        ' database instance after ingesting from repository.'
                        )


def save_operation(op_data, staging_dir, overwrite):
    logger.info('Save the operation')
    op_uuid = op_data['id']
    dest_dir = os.path.join(
        settings.OPERATION_LIBRARY_DIR,
        op_uuid
    )
    logger.info('Destination directory for'
                f' this operation at {dest_dir}')

    # copy the cloned directory and include the .git folder
    # and any other hidden files/dirs:
    recursive_copy(staging_dir, dest_dir,
                   include_hidden=True, overwrite=overwrite)

    # overwrite the spec file just to ensure it's valid with our
    # current serializer implementation. Technically it wouldn't validate
    # if that weren't true, but we do it here either way.
    op_fileout = os.path.join(dest_dir, settings.OPERATION_SPEC_FILENAME)
    with open(op_fileout, 'w') as fout:
        fout.write(json.dumps(op_data))
