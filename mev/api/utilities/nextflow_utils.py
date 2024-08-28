import glob
import os
import re
import json
import logging

from api.utilities.executed_op_utilities import get_execution_directory_path
from api.utilities.basic_utils import read_local_file

NF_SUFFIX = '.nf'

logger = logging.getLogger(__name__)

# when the nextflow tool communicates back via POST request,
# it includes an 'event' field which describes the current state
# of the submission. We use these keywords to mark the states
# of the run.
NEXTFLOW_STARTED = 'started'
NEXTFLOW_PROCESS_SUBMITTED = 'process_submitted'
NEXTFLOW_PROCESS_STARTED = 'process_started'
NEXTFLOW_PROCESS_COMPLETED = 'process_completed'
NEXTFLOW_COMPLETED = 'completed'
NEXTFLOW_ERROR = 'error'

# a list of the job status options:
JOB_STATES = [
    NEXTFLOW_STARTED,
    NEXTFLOW_PROCESS_SUBMITTED,
    NEXTFLOW_PROCESS_STARTED,
    NEXTFLOW_PROCESS_COMPLETED,
    NEXTFLOW_COMPLETED,
    NEXTFLOW_ERROR
]

# Used for more readable messages on the frontend
READABLE_STATES = {
    NEXTFLOW_STARTED: 'Job has been started',
    NEXTFLOW_PROCESS_SUBMITTED: 'Submitted job to queue',
    NEXTFLOW_PROCESS_STARTED: 'Job is running',
    NEXTFLOW_PROCESS_COMPLETED: 'Job is completing',
    NEXTFLOW_COMPLETED: 'Job has finished.',
    NEXTFLOW_ERROR: 'Job submission error. An administrator has been notified.'
} 

# when a nextflow job completes, it sends a POST
# request with job metadata to a view. We save
# this metadata to a file for later use. This sets
# the canonical name for this file
FINAL_METADATA_FILENAME = 'job_metadata.json'


def get_container_names(operation_dir):
    '''
    Given a directory containing nextflow files, 
    look through those files and grab all the container
    declarations.

    Return a list of those images
    '''
    image_set = set()
    for nf_text in get_nextflow_file_contents(operation_dir).values():
        intermediate_image_set = extract_process_containers(nf_text)
        image_set = image_set.union(intermediate_image_set)
    return list(image_set)


def get_nextflow_file_contents(operation_dir):
    '''
    Finds all nextflow files in `operation_dir` and
    returns a list where each item of the list is a string
    with the file content.

    While this function seems relatively trivial, it made unit
    testing easier for other functions.
    '''
    nf_files = glob.glob(os.path.join(operation_dir, '*' + NF_SUFFIX))
    contents = {}
    for nf_file in nf_files:
        with read_local_file(nf_file) as fin:
            contents[os.path.basename(nf_file)] = fin.read()
    return contents


def extract_process_containers(nf_text):
    '''
    Given a string containing the nextflow script, 
    return a set of containers (a string declaring
    the container name and possibly a tag).
    '''
    # nextflow scripts have lines like:
    # container "docker.io/foo"
    # and the syntax requires they are on one line so
    # the following regex will grab the entire line:
    containers = re.findall('container\s+.*', nf_text)
    logger.info('From NF file, parsed the following container'
        f' definitions: {containers}')
    container_set = set()
    for c in containers:
        # c is like: container "docker.io/foo"
        # with either single or double quote possible
        match = re.search('[\'\"].*[\'\"]', c)
        start, end = match.span()
        container_id = c[start:end][1:-1]
        container_set.add(container_id)
    return container_set


def edit_nf_containers(operation_dir, name_mapping):
    '''
    `operation_dir` is a folder where all the workflow files reside
    `name_mapping` is a dict that maps the original container runtimes
    to the new names (which have the proper tagging)
    '''
    for nf_file, nf_text in get_nextflow_file_contents(operation_dir).items():
        for orig_image_str, new_image_str in name_mapping.items():
            nf_text = re.sub(orig_image_str, new_image_str, nf_text)
        with open(os.path.join(operation_dir, nf_file), 'w') as fout:
            fout.write(nf_text)


def write_final_nextflow_metadata(data, executed_operation_pk):
    '''
    When a nextflow job completes, it will POST a payload containing
    full job metadata. This function writes this to a file in the execution
    directory so that we can later refer to it for job state, metrics, etc.
    '''
    executed_op_dir = get_execution_directory_path(executed_operation_pk)
    with open(os.path.join(executed_op_dir, FINAL_METADATA_FILENAME), 'w') as fout:
        fout.write(json.dumps(data))


def read_final_nextflow_metadata(executed_operation_pk):
    '''
    Returns a dict containing the final job metadata from a nextflow-based
    execution
    '''
    executed_op_dir = get_execution_directory_path(executed_operation_pk)
    with read_local_file(
            os.path.join(executed_op_dir, FINAL_METADATA_FILENAME)) as fin:
        return json.loads(fin.read())


def job_succeeded(job_metadata):
    '''
    Returns the job success state as a boolean 
    (True if job succeeded, else False)
    '''
    return job_metadata['metadata']['workflow']['success']


def get_error_report(job_metadata):
    try:
        report = ('Your job did not successfully complete.'
                  ' Our administrators have been notified.\n')
        err = job_metadata['metadata']['workflow']['errorMessage']
        report += f'The error was: {err}'
        return report
    except KeyError as ex:
        return 'Unexpected error. An administrator has been notified.'