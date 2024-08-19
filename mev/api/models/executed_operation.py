import uuid

from django.db import models
from django.contrib.auth import get_user_model
from django.db.models import JSONField

from api.models import Operation


class ExecutedOperation(models.Model):
    '''
    An `ExecutedOperation` is an "executed" instance of a particular
    `Operation`.  The `Operation` describes what was done (i.e. which analysis)
    and the `ExecutedOperation` tracks information about the actual exection
    of that `Operation` type.
    '''

    # Some status messages to display:
    SUBMITTED = 'Submitted.'
    QUEUED = 'Queued for execution.'
    RUNNING = 'Running.'
    FINALIZING = 'Finalizing.'
    COMPLETION_SUCCESS = 'Successfully completed.'
    COMPLETION_ERROR = 'An error occurred during execution.'
    FINALIZING_ERROR = ('An error occurred while the job was finalizing.'
                        ' An administrator has been notified.'
                        )
    ADMIN_NOTIFIED = 'An administrator has been notified.'

    # This tracks the unique run in our system
    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False
    )

    # the reference to the type of Operation performed.
    operation = models.ForeignKey(
        Operation,
        on_delete=models.CASCADE
    )

    # ExecutedOperations are owned by someone.
    owner = models.ForeignKey(
        get_user_model(),
        related_name='executed_operations',
        on_delete=models.CASCADE
    )

    # This helps us locate the job itself, so we can track progress.
    # For local Docker-based jobs, this would be the container ID. For
    # remote Cromwell-based jobs, this would be the Cromwell UUID.
    # For nextflow-based jobs, we can't use UUIDs generally since they
    # may not match NF's naming requirements.
    job_id = models.CharField(
        primary_key = False, 
        default = uuid.uuid4, 
        editable = True,
        null = True,
        max_length=128
    )

    # The user-assigned name for the ExecutedOperation
    job_name = models.CharField(
        max_length = 255,
        default = ''
    )

    # The inputs to this job-- a JSON document structured as an
    # api.data_structures.UserOperationInput instance
    inputs = JSONField(null=True)

    # The outputs of the job. Initially blank, but ultimately filled 
    # with the appropriate output in the form of an
    # api.data_structures.OperationOutputmodels.Model
    outputs = JSONField(null=True)

    # error message(s) for display and help with diagnosing issues.
    error_messages = JSONField(null=True)

    # The job status-- for displaying updates/current status (e.g. "running").
    # Depends on which runner, etc.
    status = models.CharField(
        max_length = 255,
        default = ''
    )

    # When did the Operation start
    execution_start_datetime = models.DateTimeField(
        auto_now_add = True
    )

    # When did the Operation stop, even if unsuccessful.
    # Note that this is set after the "wrap-up" operations have concluded.
    # Not to be used as a way to get accurate run times for the analyses.
    execution_stop_datetime = models.DateTimeField(null=True)

    # whether the job failed or not
    job_failed = models.BooleanField(default=False)

    # flag marking whether the job is in the process of "finalizing"
    # This way, repeated requests do not trigger multiple attempts to
    # wrap-up an ExecutedOperation. Note that a repeated request with a 
    # very short interval (i.e. less than it takes to save a database model)
    # could trigger a race condition, but there does not seem to be a way
    # around this.
    is_finalizing = models.BooleanField(default=False)

    # the run mode. Saves another lookup (via the FK to Operation table)
    # so we don't have to load the operation spec to get the run mode:
    mode = models.CharField(null=False, max_length = 100)
