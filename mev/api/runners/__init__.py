import logging

from exceptions import JobSubmissionException

from .local_docker import LocalDockerRunner
from .nextflow import LocalNextflowRunner, \
    AWSBatchNextflowRunner
from .remote_cromwell import RemoteCromwellRunner
from .ecs import ECSRunner

logger = logging.getLogger(__name__)

RUNNER_MAPPING = {
    LocalDockerRunner.NAME: LocalDockerRunner,
    AWSBatchNextflowRunner.NAME: AWSBatchNextflowRunner,
    LocalNextflowRunner.NAME: LocalNextflowRunner,
    RemoteCromwellRunner.NAME: RemoteCromwellRunner,
    ECSRunner.NAME: ECSRunner
}
AVAILABLE_RUNNERS = list(RUNNER_MAPPING.keys())


def get_runner(run_mode):
    '''
    Given the run mode (as given in an operation spec),
    return the class that implements that job running method.
    '''
    try:
        return RUNNER_MAPPING[run_mode]
    except KeyError as ex:
        logger.error(f'Requested an unknown run mode: {run_mode}')
        raise ex
 

def submit_job(executed_op, op, validated_inputs):
    '''
    Submits the job to the proper runner.

    `executed_op` is an instance of ExecutedOperation (database model)
    `op` is an instance of data_structures.operation.Operation
    `validated_inputs` is a dict of inputs. Each key matches a key 
      from the `op_data` and the value is an instance of `UserOperationInput`
    '''
    runner_class = get_runner(executed_op.mode)
    runner = runner_class()
    try:
        runner.run(executed_op, op, validated_inputs)
    except Exception as ex:
        logger.error(f'Caught a job submission exception:\n{ex}')
        raise JobSubmissionException(f'Failed to submit job {executed_op.pk}')


def finalize_job(executed_op, op):
    '''
    Finalizes the job using the proper runner.

    `executed_op` is an instance of ExecutedOperation (database model)
    `op` is an instance of data_structures.operation.Operation
    '''
    runner_class = get_runner(executed_op.mode)
    runner = runner_class()
    runner.finalize(executed_op, op)
