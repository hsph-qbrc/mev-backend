import logging

#from celery.decorators import task
from celery import shared_task
import api.utilities.resource_utilities as resource_utilities
from api.storage_backends import get_storage_backend
from api.models import Resource

logger = logging.getLogger(__name__)

@shared_task(name='delete_file')
def delete_file(path):
    '''
    Deletes a file.  Can be a local or remote resource.
    '''
    logger.info('Requesting deletion of {path}'.format(path=path))
    get_storage_backend().delete(path)

@shared_task(name='validate_resource')
def validate_resource(resource_pk, requested_resource_type):
    '''
    This function only performs validation of the resource
    '''
    resource = resource_utilities.get_resource_by_pk(resource_pk)

    try:
        resource_utilities.validate_resource(resource, requested_resource_type)
    except Exception as ex:
        resource.status = Resource.UNEXPECTED_VALIDATION_ERROR

    # regardless of what happened above, set the 
    # status to be active (so changes can be made)
    # and save the instance
    resource.is_active = True
    resource.save()


@shared_task(name='validate_resource_and_store')
def validate_resource_and_store(resource_pk, requested_resource_type):
    '''
    This function handles the background validation of uploaded
    files.

    Previous to calling this function, we set the `is_active` flag
    to False so that the `Resource` is disabled for use.
    '''
    resource = resource_utilities.get_resource_by_pk(resource_pk)
    try:
        resource_utilities.validate_and_store_resource(resource, requested_resource_type)
    except Exception as ex:
        resource.status = Resource.UNEXPECTED_VALIDATION_ERROR
