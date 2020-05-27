import os
import uuid
import logging

from django.conf import settings

from api.models import Resource
from .basic_utils import make_local_directory, \
    move_resource, \
    copy_local_resource

logger = logging.getLogger(__name__)

def check_for_resource_operations(resource_instance):
    '''
    To prevent deleting critical resources, we check to see if a
    `Resource` instance has been used for any operations within a
    `Workspace`.  If it has, return True.  Otherwise return False.
    '''
    pass

def create_resource_from_upload(filepath, 
    filename, 
    resource_type, 
    is_public,
    is_local,
    owner):
    '''
    Creates and returns a Resource instance.
    `filepath` is a path to the file-based resource.  Could be local or in bucket-based
    storage.
    `filename` is a string, extracted from the name of the uploaded file.
    `resource_type` is one of the acceptable resource type identifiers.  
       - At this point, it's just a "requested" resource type and has not
         been verified.
    `is_public` is a boolean.
    `owner` is an instance of our user model
    '''
    # create a serialized representation so we can use the validation
    # contained there.  Note that since we are creating a new Resource
    # we have NOT validated it.  Hence, the `resource_type` is set to null.
    d = {'owner_email': owner.email,
        'path': filepath,
        'name': filename,
        'resource_type': resource_type,
        'is_public': is_public,
        'is_local': is_local
    }

    from api.serializers import ResourceSerializer
    rs = ResourceSerializer(data=d)

    # values were checked prior to this, but we enforce this again
    if rs.is_valid(raise_exception=True):
        r = rs.save()
        return r

def set_resource_to_validation_status(resource_instance):
    '''
    Function created to temporarily "disable"
    a Resource while it is pending verification of the
    type.  
    '''
    resource_instance.status = Resource.VALIDATING
    resource_instance.is_active = False
    resource_instance.save()


def move_resource_to_final_location(resource_instance):
    '''
    Given a `Resource` instance (validated), create an appropriate
    space for the resource
    '''

    # create a final file location by concatenating the
    # resource UUID and the file's "human readable" name
    owner_uuid = str(resource_instance.owner.user_uuid)
    basename = '{uuid}.{name}'.format(
        uuid=resource_instance.pk, 
        name=resource_instance.name
    )

    if resource_instance.is_local:
        user_dir = os.path.join(
            settings.USER_STORAGE_DIR,
            owner_uuid
        )
        if not os.path.exists(user_dir):
            # in the unlikely event the directory cannot be made
            # this function will raise an exception, which will be
            # caught in the calling function
            make_local_directory(user_dir)

        final_path = os.path.join(
            user_dir,
            basename
        )
        # if the file cannot be moved, the following function may raise an exception.
        # We catch in the calling function
        final_path = move_resource(resource_instance.path, final_path)
        return final_path

    else:
        # TODO: implement
        raise NotImplementedError('Implement logic for remote resource')


def copy_resource_to_workspace(unattached_resource, workspace):
    '''
    This function handles the copy of an existing (and validated)
    Resource when it is added to a Workspace.

    It copies both the physical resource and creates the appropriate
    database object.
    '''
    logger.info('Adding resource ({resource}) to'
        ' workspace ({workspace}).'.format(
            workspace = str(workspace),
            resource = str(unattached_resource)
        )
    )  

    # We will eventually use the `move_resource_to_final_location`
    # function above.  However, that essentially performs a "mv"
    # on a file (and we want to retain the original resource).  
    # Therefore, we will perform a copy to some temp location
    # and then call that function.
    tmp_path = os.path.join(settings.TMP_DIR, str(uuid.uuid4()))
    tmp_path = copy_local_resource(unattached_resource.path, tmp_path) 

    # we need to create a new Resource with the Workspace 
    # field filled appropriately.  Note that this method of "resetting"
    # the primary key by setting it to None creates an effective copy
    # of the original resource. We then alter the path field and save.
    r = unattached_resource
    r.pk = None
    r.path = tmp_path
    r.workspace = workspace
    r.is_public = False # when we copy to a workspace, set private
    r.save()

    # finally, move the copy currently sitting in the temp dir to the final
    # location.  Again, this is essentially a copy of the original file, but
    # with a different primary key and an entirely identical file
    final_path = move_resource_to_final_location(r)
    r.path = final_path
    r.save()

    return r