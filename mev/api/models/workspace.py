import uuid

from django.contrib.auth import get_user_model
from django.db import models

class Workspace(models.Model):
    '''
    A `Workspace` is a way to logically group the files and and analyses
    that are part of a user's work.

    Users can have multiple `Workspace`s to separate distinct analyses.

    Data, files, and analyses are grouped under a `Workspace` such that all
    information related to the analyses, including analysis history, is captured
    the `Workspace`.
    '''

    # Workspace instances will be referenced by their UUID instead of a PK
    id = models.UUIDField(
        primary_key = True, 
        default = uuid.uuid4, 
        editable = False
    )

    # Workspace instances must be owned by someone
    owner = models.ForeignKey(
        get_user_model(), 
        related_name = 'workspaces', 
        on_delete = models.CASCADE
    )

    # Users can give the workspace a name
    workspace_name = models.CharField(
        max_length = 100,
        default = ''  
    )

    creation_datetime = models.DateTimeField(
        auto_now_add = True
    )

    def save(self, *args, **kwargs):
        '''
        This overrides the save method, implementing
        custom behavior upon creation
        '''

        if self._state.adding and ((self.workspace_name is None) or (self.workspace_name == '')):
            # Initially set the workspace_name to the UUID
            # Users can later edit that to something more memorable
            self.workspace_name = str(self.id)
        super().save(*args, **kwargs)

    def __str__(self):
        return '''Workspace ({uuid})
          Name: {name}
          Owner: {owner}
          Created: {date}'''.format(
                name = self.workspace_name,
                uuid = str(self.id),
                owner = str(self.owner),
                date = self.creation_datetime
            )

    class Meta:
        unique_together = (

            # The name of the workspaces should be unique for 
            # each user
            ('owner','workspace_name'),
        )
