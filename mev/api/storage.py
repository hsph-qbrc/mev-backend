import uuid
import os
from io import BytesIO
import datetime
import logging

import boto3
import botocore
from storages.backends.s3boto3 import S3Boto3Storage
from django.conf import settings
from django.core.files import File
from django.core.files.storage import FileSystemStorage, \
    storages

from exceptions import StorageException

from api.utilities.basic_utils import copy_local_resource
from api.utilities.resource_utilities import create_resource
from api.utilities.admin_utils import alert_admins

logger = logging.getLogger(__name__)


S3_PREFIX = 's3://'

class LocalResourceStorage(FileSystemStorage):

    def check_if_exists(self, full_path):
        '''
        Trivial in the case of local storage, but
        implemented to keep a consistent interface
        with the remote storage classes which 
        implement this method (so as not to shadow the
        `exists` methods provided by those).
        '''
        return super().exists(full_path)

    def get_absolute_path(self, path_relative_to_storage_root):
        return super().path(path_relative_to_storage_root)

    def localize(self, resource, local_dir):
        '''
        Copies the file/resource from local filesystem storage into
        a local directory.

        This avoids conditionals when local processes (e.g. docker containers)
        need to use a file. We don't have to check whether we are using local
        or remote storage.
        '''
        name = str(uuid.uuid4())
        dest_path = os.path.join(local_dir, name)
        copy_local_resource(resource.datafile.path, dest_path)

    def copy_to_bucket(self, resource, dest_bucket_name, dest_object=None):
        raise NotImplementedError('Since local storage is used, we do not allow'\
            ' interaction with bucket/object storage.')

    def copy_to_storage(self, src_bucket, src_object, dest_object):
        raise NotImplementedError('Since local storage is used, we do not allow'\
            ' interaction with bucket/object storage.')

    def create_resource_from_interbucket_copy(self, owner, src_path):
        raise NotImplementedError('Since local storage is used, we do not allow'\
            ' interaction with bucket/object storage.')

            
class S3ResourceStorage(S3Boto3Storage):
    bucket_name = settings.MEDIA_ROOT

    # If True, this will append random chars to the end of the object name
    # so that files are not overwritten
    file_overwrite = False

    def _get_local_storage(self):
        return storages['local']

    def _open(self, name, mode="rb"):
        '''
        This override of _open performs local caching of 
        files when they are accessed.

        If the file does not exist locally, we use local storage
        to save the S3-based file to our local storage. If the
        file does exist locally, we return the handle referring
        to the local version.

        Since WebMeV files are not directly edited, there is no 
        risk of cache issues due to out-of-date files, etc.
        '''

        # leverage the local file system storage impl:
        local_sc = self._get_local_storage()

        # if the resource is found in local storage, use that
        if local_sc.exists(name):
            f = local_sc.open(name, mode)
        # otherwise, save to the local cache for later requests
        else:
            # use the open method of the django-storages S3 implementation
            f = super()._open(name, mode)
            try:
                # using that handle `f`, save a copy to the local cache
                local_sc.save(name, f)

                # the save above closes f implicitly, so we need to
                # re-open before returning. Use the new local version here
                f = local_sc.open(name, mode)
            except Exception as ex:
                alert_admins(f'Caught an exception when attempting to cache: {ex}')
        return f

    def _save(self, name, content):
        return super()._save(name, content)

    def get_file_listing(self, full_path, recurse=False):
        '''
        This is a semi-override of the `listdir` method which returns a 
        list of fully-resolved paths to S3-based files

        Note that we do NOT override the `listdir` method itself to avoid
        unintended effects of shadowing the default S3Boto3Storage API.
        Namely, that `listdir` method expects a path relative to the storage
        root so we can't use it to pull files from OTHER buckets associated
        with the WebMeV app. This permits that interaction without having
        various boto3 library calls scattered in the codebase.

        Additionally, the `listdir` method returns a tuple of directory
        lists and file lists. Here we only return a list of files.
        '''
        # in case we decide to implement in the future. Block for now
        # and allow only files at the first-level of the passed path.
        if recurse:
            raise NotImplementedError('')

        # in principle, a user of this function is passing the full path to
        # a directory. Given the S3 architecture, the directory is still an object
        # so we can use the method below. We name the "remainder" of the path
        # suggestively since it's supposed to be a prefix to any objects we 
        # are looking for in said directory.
        b, prefix = self.get_bucket_and_object_from_full_path(full_path)
        # if we're dealing with something in our "default" storage
        # bucket, we can simply call the parent method
        if b == self.bucket_name:
            dirs, files = super().listdir(prefix)
            return [f'{S3_PREFIX}{b}/{x}' for x in files]
        else:
            # This `else` handles situations where we want to look
            # into other buckets (e.g. one that stores nextflow scratch
            # files, etc.)
            s3 = boto3.resource('s3')
            bucket_obj = s3.Bucket(b)
            returned_paths = []
            bucket_contents = bucket_obj.objects.filter(Prefix=prefix)
            try:
                for obj in bucket_contents:
                    # by default, the "directory" itself
                    # is an object. Don't care about that.
                    if obj.key == prefix:
                        continue
                    returned_paths.append(f'{S3_PREFIX}{b}/{obj.key}')
            except botocore.exceptions.ClientError as ex:
                err = 'Caught an exception when listing the following' \
                      f' path: {full_path}. Error was {ex}'
                alert_admins(err)
                return []
            return returned_paths

    def check_if_exists(self, full_path):
        '''
        This is a semi-override of the `exists` method which returns a 
        boolean indicating whether a S3-based file exists.

        Note that we do NOT override the `exists` method itself to avoid
        unintended effects of shadowing the default S3Boto3Storage API.
        Instead, we extend the capabilities to check for object existence
        in buckets BEYOND the single bucket that is tied to this class.
        Recall that the default use-case of django storages is to
        provide a storage mechanism that is associated with a single bucket.
        WebMeV has additional buckets it interacts with and this
        permits that interaction without having various boto3 library calls
        scattered in the codebase.
        '''
        b, obj = self.get_bucket_and_object_from_full_path(full_path)

        # if we're dealing with something in our "default" storage
        # bucket, we can simply call the parent method
        if b == self.bucket_name:
            return super().exists(obj)
        else:
            # This `else` handles situations where we want to look
            # into other buckets (e.g. one that stores nextflow scratch
            # files, etc.)
            s3 = boto3.resource('s3')
            try:
                # does a HEAD request so it's quick:
                s3.Object(b, obj).load()
            except botocore.exceptions.ClientError as ex:
                if (ex.response['Error']['Code'] == '404') \
                    or (ex.response['Error']['Code'] == 404):
                    return False
                else:
                    err = 'Received an unexpected response' \
                        f' when querying object existence: {ex.response}'
                    logger.error(err)
                    alert_admins(err)
                    return False
            except Exception as ex:
                err = 'Unexpected exception when' \
                    f' querying object existence: {ex}'
                logger.error(err)
                alert_admins(err)
                return False
            return True

    def get_absolute_path(self, path_relative_to_storage_root):
        '''
        Returns the "full" s3:// 'path'.

        Used for situations like Cromwell, which is looking for a 
        full path to an object.

        `path_relative_to_storage_root` is typically from the `name`
        attribute of the FileField of the Resource class, e.g.
        r.datafile.name
        '''
        return f'{S3_PREFIX}{self.bucket_name}/{path_relative_to_storage_root}'

    def get_bucket_and_object_from_full_path(self, full_path):
        '''
        Given `full_path` (e.g. s3://my-bucket/folderA/file.txt)
        return a tuple of the bucket (`my-bucket`) and the object
        (`folderA/file.txt`)
        '''
        if not full_path.startswith(S3_PREFIX):
            raise Exception(f'The full path must \
                include the prefix {S3_PREFIX}')
        return full_path[len(S3_PREFIX):].split('/', 1)

    def localize(self, resource, local_dir):
        '''
        Downloads the file/resource from S3 storage into
        a local directory and returns the path on the local
        filesystem
        '''
        name = str(uuid.uuid4())
        dest_path = os.path.join(local_dir, name)
        s3 = boto3.client('s3')
        s3.download_file(settings.MEDIA_ROOT, resource.datafile.name, dest_path)
        return dest_path

    def _copy(self, src_bucket, dest_bucket, src_object, dest_object):
        '''
        A "private" method for general copies. Other public methods expose
        copies in and out of our storage. Use those methods instead.
        
        This performs a managed copy, which will perform multipart copy 
        if necessary (forfiles > 5Gb). 
        See https://boto3.amazonaws.com/v1/documentation/api/latest/reference\
            /services/s3.html#S3.Client.copy
        '''

        #TODO: catch bucket access issues
        s3 = boto3.resource('s3')
        copy_source = {
            'Bucket': src_bucket,
            'Key': src_object
        }
        try:
            s3.meta.client.copy(copy_source, dest_bucket, dest_object)
        except botocore.exceptions.ClientError as ex:
            response_code = ex.response['Error']['Code']
            if response_code == '404':
                raise FileNotFoundError
        except Exception:
            raise StorageException('Unexpected error when performing a'
                ' bucket-to-bucket copy.'
            )
        return os.path.join(
            dest_bucket,
            dest_object
        )

    def copy_to_storage(self, src_bucket, src_object, dest_object=None):
        '''
        Copies from a bucket outside of Django storage into our
        django-managed S3 storage

        `dest_object` is relative to our django-managed storage bucket.
        If `dest_object` is None, we take the basename of the source object

        '''
        if dest_object is None:
            dest_object = os.path.basename(src_object)

        return self._copy(
            src_bucket,
            self.bucket_name,
            src_object,
            dest_object
        )

    def create_resource_from_interbucket_copy(self, owner, src_path):
        '''
        Copies an object into our storage and creates/returns
        a Resource instance.

        `src_path` is the FULL bucket path, e.g. s3://<BUCKET>/<object>
        '''
        src_bucket, src_object = self.get_bucket_and_object_from_full_path(src_path)
        logger.info(f'src_bucket: {src_bucket}')
        logger.info(f'src_object: {src_object}')
        # To avoid needing to duplicate the logic of locating files
        # within our storage, we basically create a dummy placeholder
        # "file" with empty content and create an instance
        # of api.models.Resource. We then get the path of that dummy
        # file and use that as a place to send the copy of our 
        # bucket-based file.
        with BytesIO() as fh:
            f = File(fh, str(uuid.uuid4()))
            r = create_resource(
                owner,
                file_handle=f,
                name=os.path.basename(src_object),
                # initially inactive so that a user can't interact with it (yet)
                is_active=False
            )
        dest_obj = r.datafile.name
        logger.info('For interbucket copy, empty placeholder is'
            f' located at: {dest_obj}')
        try:
            self.copy_to_storage(
                src_bucket,
                src_object,
                dest_obj
            )        
            logger.info('Copy operation completed.')
            # now that the file is copied to the correct location, update the 
            # api.models.Resource instance in the database.
            r.is_active = True
            r.size = r.datafile.size
            r.save()
            return r
        except Exception as ex:
            logger.info('Caught an exception during in intrabucket copy.')
            # Need to delete that placeholder file and the database record
            self.delete(r.datafile.name)
            r.delete()
            raise ex

    def copy_out_to_bucket(self, resource, dest_bucket_name, dest_object=None):
        '''
        Copies the resource to a destination at
        s3://<dest_bucket_name>/<dest_object>

        Since api.models.Resource objects can only reside in our
        storage bucket (see `bucket_name` above), this is a copy
        AWAY from our storage.

        If `dest_object` is None, we create a new UUID-based name
        '''
        if dest_object is None:
            dest_object = str(uuid.uuid4())

        return self._copy(
            self.bucket_name,
            dest_bucket_name,
            resource.datafile.name,
            dest_object
        )

    def wait_until_exists(self, full_path):
        '''
        For any full path (e.g. s3://<bucket>/<object>), call the 
        boto3 `wait_until_exists`. This works on any bucket to which
        the host ec2 instance has access
        '''
        s3 = boto3.resource('s3')
        bucket_name, obj_name = self.get_bucket_and_object_from_full_path(full_path)
        obj = s3.Object(bucket_name, obj_name)
        try:
            logger.info(f'Checking for {full_path}')
            t0 = datetime.datetime.now()
            obj.wait_until_exists()
        except botocore.exceptions.WaiterError as ex:
            t1 = datetime.datetime.now()
            logger.info(f'After waiting {t1-t0}, still could not find'
                f' an object at {full_path}')
            raise FileNotFoundError

    def delete_object(self, full_path):
        s3 = boto3.client('s3')
        bucket_name, obj_name = self.get_bucket_and_object_from_full_path(full_path)
        s3.delete_object(Bucket=bucket_name, Key=obj_name)
        