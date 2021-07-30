import os
import logging

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from api.utilities.basic_utils import make_local_directory, \
    move_resource, \
    delete_local_file
from .base import BaseStorageBackend
from .helpers import localize_remote_resource

try:
    STORAGE_DIRNAME = os.environ['LOCAL_STORAGE_DIRNAME']
except KeyError as ex:
    raise ImproperlyConfigured('Need to supply the following environment'
        ' variable: {k}'.format(k=ex))

logger = logging.getLogger(__name__)

class LocalStorage(BaseStorageBackend):

    is_local_storage = True

    def store(self, resource_instance):
        '''
        Handles moving the file described by the `resource_instance`
        arg to its final location.
        '''
        relative_path = self.construct_relative_path(resource_instance)

        # where all user files are kept locally:
        base_storage_dir = os.path.join(settings.DATA_DIR, STORAGE_DIRNAME)

        # the final location of this file on our local storage:
        destination = os.path.join(base_storage_dir, relative_path)

        storage_dir = os.path.dirname(destination)
        if not os.path.exists(storage_dir):

            # this function can raise an exception which will get
            # pushed up to the caller
            make_local_directory(storage_dir)

        # storage directory existed.  Move the file:
        source = resource_instance.path

        if os.path.exists(source): # if on the local filesystem
            move_resource(source, destination)
            return destination
        else:
            # NOT on the local filesystem. go get it.
            return localize_remote_resource(resource_instance)

    def delete(self, path):
        delete_local_file(path)

    def get_filesize(self, path):
        try:
            return os.path.getsize(path)
        except FileNotFoundError:
            logger.error('Failed to get the size of local file at {path}'
            ' since it did not exist.'.format(path=path))
        except Exception as ex:
            logger.error('Caught some unexpected exception when calling'
            ' os.path.getsize.  Exception was {ex}'.format(ex=ex))
        # since file-size is not "critical", we log the errors and just
        # return 0 since it will still work.
        return 0

    def resource_exists(self, path):
        '''
        Returns true/false for whether the file exists at the path
        '''
        return os.path.exists(path)

    def get_local_resource_path(self, resource_instance):
        '''
        Returns the path to the file resource on the local machine.
        Trivial for this implementation of local storage
        '''
        return resource_instance.path

    def get_download_url(self, resource_instance):
        '''
        Returns a url that will allow download of this resource.
        '''
        return self.get_local_resource_path(resource_instance)
