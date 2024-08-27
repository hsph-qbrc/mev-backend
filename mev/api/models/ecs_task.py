from django.db import models

from api.models import Operation


class ECSTaskDefinition(models.Model):
    '''
    Tracks association between WebMEV api.models.Operation
    and the task ARN.
    '''
    task_arn = models.CharField(
        null = False,
        max_length=256
    )

    operation = models.ForeignKey(
        Operation,
        on_delete=models.CASCADE
    )

    # updates to the task definition will create
    # a new ARN, but this will easily allow us to
    # sort to retrieve only the most recent revision.
    revision_date = models.DateTimeField(
        auto_now_add = True
    )