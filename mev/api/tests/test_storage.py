import os
import shutil
import unittest
import unittest.mock as mock

from django.conf import settings

from api.storage import S3ResourceStorage, \
    LocalResourceStorage
from botocore.exceptions import ClientError


class TestLocalResourceStorage(unittest.TestCase):

    def test_existence_method(self):
        '''
        This is not so much a test per se as a double-check
        that our storage interface matches between the local
        and remote storage classes.
        '''
        storage = LocalResourceStorage()
                
        fpath = os.path.join(
            os.path.dirname(__file__),
            'resource_contents_test_files',  
            'demo_file1.tsv'
        )
        f = open(fpath)
        fname = storage.save('abc.txt', f)
        actual_storage_path = storage.path(fname)

        self.assertTrue(storage.check_if_exists(fname))
        self.assertTrue(storage.check_if_exists(actual_storage_path))

        # clean up- otherwise files get left around
        storage.delete(fname)

    def test_abs_path(self):
        '''
        This is not so much a test per se as a double-check
        that our storage interface matches between the local
        and remote storage classes.
        '''
        storage = LocalResourceStorage()
                
        fpath = os.path.join(
            os.path.dirname(__file__),
            'resource_contents_test_files',  
            'demo_file1.tsv'
        )
        f = open(fpath)
        fname = storage.save('abc.txt', f)
        actual_storage_path = storage.path(fname)

        ffp1 = storage.get_absolute_path(fname)
        self.assertEqual(ffp1, actual_storage_path)

        ffp2 = storage.get_absolute_path(actual_storage_path)
        self.assertEqual(ffp2, actual_storage_path)

        # clean up- otherwise files get left around
        storage.delete(fname)


class TestS3ResourceStorage(unittest.TestCase):
    '''
    Tests the overridden methods functions of the S3ResourceStorage class.

    Note that most methods are thin wrappers on Django Storage's S3boto3Storage
    so we are not aiming to re-test that package. Rather just testing that
    the proper calls are made to that api.
    '''

    @mock.patch('api.storage.S3Boto3Storage.exists')
    def test_existence_in_main_storage(self, mock_base_exists):
        '''
        Tests that our `check_if_exists` makes the proper calls
        and handles unexpected situations properly
        '''
        storage = S3ResourceStorage()
        mock_get_bucket_and_object_from_full_path = mock.MagicMock()
        mock_bucket_name = 'my-bucket'
        mock_object_name = 'some/object.txt'
        mock_get_bucket_and_object_from_full_path.return_value = (mock_bucket_name, mock_object_name)
        storage.get_bucket_and_object_from_full_path = mock_get_bucket_and_object_from_full_path

        # check that we call the parent method if we are checking a file in
        # the "main" Django storage bucket
        storage.bucket_name = mock_bucket_name
        storage.check_if_exists(f's3://{mock_bucket_name}/{mock_object_name}')
        mock_base_exists.assert_called_once_with(mock_object_name)

    @mock.patch('api.storage.boto3')
    def test_existence_in_other_storage(self, mock_boto):
        '''
        Tests that our `check_if_exists` makes the proper calls
        and handles unexpected situations properly
        '''
        storage = S3ResourceStorage()
        mock_get_bucket_and_object_from_full_path = mock.MagicMock()
        # test if we are checking an object in another bucket
        mock_other_bucket_name = 'other-bucket'
        mock_object_name = 'some/object.txt'
        mock_get_bucket_and_object_from_full_path.return_value = (mock_other_bucket_name, mock_object_name)
        mock_s3 = mock.MagicMock()
        mock_s3_object = mock.MagicMock()
        mock_s3.Object.return_value = mock_s3_object
        mock_boto.resource.return_value = mock_s3
        storage.check_if_exists(f's3://{mock_other_bucket_name}/{mock_object_name}')
        mock_s3.Object.assert_called_once_with(mock_other_bucket_name, mock_object_name)
        mock_s3_object.load.assert_called()

    @mock.patch('api.storage.alert_admins')
    @mock.patch('api.storage.boto3')
    def test_existence_not_found_in_storage(self, 
            mock_boto,
            mock_alert_admins):
        '''
        Tests that our `check_if_exists` handles an unexpected exception
        '''
        storage = S3ResourceStorage()
        mock_get_bucket_and_object_from_full_path = mock.MagicMock()
        # test if we are checking an object in another bucket
        mock_other_bucket_name = 'other-bucket'
        mock_object_name = 'some/object.txt'
        # test that it was not found (404 response)
        mock_other_bucket_name = 'other-bucket'
        mock_get_bucket_and_object_from_full_path.return_value = (mock_other_bucket_name, mock_object_name)
        mock_s3 = mock.MagicMock()
        mock_s3_object = mock.MagicMock()
        mock_s3_object.load.side_effect = ClientError(
            {'Error': {'Code': 404, 'Message': 'abc'}},
            'load_object'
        )
        mock_s3.Object.return_value = mock_s3_object
        mock_boto.resource.return_value = mock_s3
        was_found = storage.check_if_exists(f's3://{mock_other_bucket_name}/{mock_object_name}')
        self.assertFalse(was_found)
        mock_s3.Object.assert_called_once_with(mock_other_bucket_name, mock_object_name)
        mock_s3_object.load.assert_called()
        mock_alert_admins.assert_not_called()

    @mock.patch('api.storage.alert_admins')
    @mock.patch('api.storage.boto3')
    def test_non_404_response_in_storage(self,
            mock_boto,
            mock_alert_admins):
        '''
        Tests that our `check_if_exists` handles an unexpected exception
        '''
        storage = S3ResourceStorage()
        mock_get_bucket_and_object_from_full_path = mock.MagicMock()
        # test if we are checking an object in another bucket
        mock_other_bucket_name = 'other-bucket'
        mock_object_name = 'some/object.txt'
        # test that it was not found (404 response)
        mock_other_bucket_name = 'other-bucket'
        mock_get_bucket_and_object_from_full_path.return_value = (mock_other_bucket_name, mock_object_name)
        mock_s3 = mock.MagicMock()
        mock_s3_object = mock.MagicMock()
        mock_s3_object.load.side_effect = ClientError(
            {'Error': {'Code': 500, 'Message': 'abc'}},
            'load_object'
        )
        mock_s3.Object.return_value = mock_s3_object
        mock_boto.resource.return_value = mock_s3
        was_found = storage.check_if_exists(f's3://{mock_other_bucket_name}/{mock_object_name}')
        self.assertFalse(was_found)
        mock_s3.Object.assert_called_once_with(mock_other_bucket_name, mock_object_name)
        mock_s3_object.load.assert_called()
        mock_alert_admins.assert_called()

    @mock.patch('api.storage.alert_admins')
    @mock.patch('api.storage.boto3')
    def test_existence_raises_ex(self,
            mock_boto,
            mock_alert_admins):
        '''
        Tests that our `check_if_exists` handles an unexpected exception
        '''
        storage = S3ResourceStorage()
        mock_get_bucket_and_object_from_full_path = mock.MagicMock()
        # test if we are checking an object in another bucket
        mock_other_bucket_name = 'other-bucket'
        mock_object_name = 'some/object.txt'
        # test that it was not found (404 response)
        mock_other_bucket_name = 'other-bucket'
        mock_get_bucket_and_object_from_full_path.return_value = (mock_other_bucket_name, mock_object_name)
        mock_s3 = mock.MagicMock()
        mock_s3_object = mock.MagicMock()
        mock_s3_object.load.side_effect = Exception('!!!')
        mock_s3.Object.return_value = mock_s3_object
        mock_boto.resource.return_value = mock_s3
        was_found = storage.check_if_exists(f's3://{mock_other_bucket_name}/{mock_object_name}')
        self.assertFalse(was_found)
        mock_s3.Object.assert_called_once_with(mock_other_bucket_name, mock_object_name)
        mock_s3_object.load.assert_called()
        mock_alert_admins.assert_called()

    @mock.patch('api.storage.S3Boto3Storage.listdir')
    def test_listing_in_main_storage(self, mock_base_listdir):
        '''
        Tests that our `get_file_listing` makes the proper calls
        and handles unexpected situations properly
        '''
        storage = S3ResourceStorage()
        mock_get_bucket_and_object_from_full_path = mock.MagicMock()
        mock_bucket_name = 'my-bucket'
        mock_dir_name = 'some-dir/'
        mock_get_bucket_and_object_from_full_path.return_value = (mock_bucket_name, mock_dir_name)
        storage.get_bucket_and_object_from_full_path = mock_get_bucket_and_object_from_full_path

        mock_base_listdir.return_value = ([],['a.txt', 'b.txt'])
        # check that we call the parent method if we are checking a file in
        # the "main" Django storage bucket
        storage.bucket_name = mock_bucket_name
        result = storage.get_file_listing(f's3://{mock_bucket_name}/{mock_dir_name}')
        self.assertCountEqual(result, [
            f's3://{mock_bucket_name}/a.txt',
            f's3://{mock_bucket_name}/b.txt'
        ])
        mock_base_listdir.assert_called_once_with(mock_dir_name)
   
    @mock.patch('api.storage.alert_admins')
    @mock.patch('api.storage.boto3')
    def test_listing_on_invalid_bucket(self,
            mock_boto,
            mock_alert_admins):
        '''
        Tests that our `get_file_listing` handles a bad bucket name
        (which can include buckets to which the instance does not have
        access to)
        '''
        storage = S3ResourceStorage()
        mock_get_bucket_and_object_from_full_path = mock.MagicMock()
        # test if we are checking an object in another bucket
        mock_other_bucket_name = 'other-bucket'
        mock_dir = 'some-dir/'

        mock_s3 = mock.MagicMock()
        mock_bucket_object = mock.MagicMock()
        mock_collection = mock.MagicMock()
        # when using the real API, it's lazy so you only get an
        # exception thrown when you attempt to iterate over the collection
        mock_collection.__iter__.side_effect = ClientError(
            {'Error': {'Code': 404, 'Message': 'No such bucket'}},
            '_'
        )
        mock_bucket_object.objects.filter.return_value = mock_collection
        mock_s3.Bucket.return_value = mock_bucket_object
        mock_boto.resource.return_value = mock_s3

        files = storage.get_file_listing(f's3://{mock_other_bucket_name}/{mock_dir}')
        self.assertTrue(len(files) == 0)
        mock_alert_admins.assert_called()

    @mock.patch('api.storage.alert_admins')
    @mock.patch('api.storage.boto3')
    def test_listing_on_valid_bucket(self,
            mock_boto,
            mock_alert_admins):
        '''
        Tests that our `get_file_listing` works/returns as expected
        '''
        storage = S3ResourceStorage()
        mock_other_bucket_name = 'other-bucket'
        mock_dir = 'some-dir/'
        mock_get_bucket_and_object_from_full_path = mock.MagicMock()
        mock_get_bucket_and_object_from_full_path.return_value = (mock_other_bucket_name, mock_dir)
        # test if we are checking an object in another bucket

        mock_s3 = mock.MagicMock()
        mock_bucket_object = mock.MagicMock()
        item1 = mock.MagicMock()
        item1.key = mock_dir
        item2 = mock.MagicMock()
        f2 = f'{mock_dir}abc.txt'
        item2.key = f2
        item3 = mock.MagicMock()
        f3 = f'{mock_dir}xyz.txt'
        item3.key = f3
        mock_collection = [item1, item2, item3]
        mock_bucket_object.objects.filter.return_value = mock_collection
        mock_s3.Bucket.return_value = mock_bucket_object
        mock_boto.resource.return_value = mock_s3

        files = storage.get_file_listing(f's3://{mock_other_bucket_name}/{mock_dir}')
        self.assertCountEqual(files,
            [f's3://{mock_other_bucket_name}/{f2}',
            f's3://{mock_other_bucket_name}/{f3}'
            ]
        )
        mock_alert_admins.assert_not_called()

    @mock.patch('api.storage.alert_admins')
    @mock.patch('api.storage.boto3')
    def test_empty_listing_on_valid_bucket(self,
            mock_boto,
            mock_alert_admins):
        '''
        Tests that our `get_file_listing` works/returns as expected
        '''
        storage = S3ResourceStorage()
        mock_other_bucket_name = 'other-bucket'
        mock_dir = 'some-dir/'
        mock_get_bucket_and_object_from_full_path = mock.MagicMock()
        mock_get_bucket_and_object_from_full_path.return_value = (mock_other_bucket_name, mock_dir)
        # test if we are checking an object in another bucket

        mock_s3 = mock.MagicMock()
        mock_bucket_object = mock.MagicMock()
        item1 = mock.MagicMock()
        item1.key = mock_dir
        mock_collection = [item1,]
        mock_bucket_object.objects.filter.return_value = mock_collection
        mock_s3.Bucket.return_value = mock_bucket_object
        mock_boto.resource.return_value = mock_s3

        files = storage.get_file_listing(f's3://{mock_other_bucket_name}/{mock_dir}')
        self.assertTrue(len(files) == 0)
        mock_alert_admins.assert_not_called()

    def test_s3_storage_caching_with_open_case1(self):
        '''
        Tests that the proper operations occur
        when the `_open` method is called. 

        Here, we test the case where the resource
        already exists in the local cache
        '''
        s3_storage = S3ResourceStorage()
        mock_get_local_storage = mock.MagicMock()
        mock_local_storage = mock.MagicMock()
        # mock that the file DOES exist in the local storage already
        mock_local_storage.exists.return_value = True
        mock_handle = mock.MagicMock()
        mock_local_storage.open.return_value = mock_handle
        mock_get_local_storage.return_value = mock_local_storage

        s3_storage._get_local_storage = mock_get_local_storage

        return_val = s3_storage._open('foo')
        self.assertEqual(return_val, mock_handle)

    @mock.patch('api.storage.S3Boto3Storage._open')
    def test_s3_storage_caching_with_open_case2(self, mock_S3Boto3Storage_open):
        '''
        Tests that the proper operations occur
        when the `_open` method is called. 

        Here, we test the case where the resource
        does NOT exist in the local cache
        '''
        s3_storage = S3ResourceStorage()


        # Need to mock the _open method of the S3Boto3Storage class
        # since we only want to check that it was called- can't interact
        # with S3 in the test suite
        mock_remote_handle = mock.MagicMock()
        mock_S3Boto3Storage_open.return_value = mock_remote_handle

        mock_get_local_storage = mock.MagicMock()
        mock_local_storage = mock.MagicMock()
        # mock that the file DOES NOT exist in the local storage already
        mock_local_storage.exists.return_value = False

        name = 'foo'
        mode = 'abc'

        mock_local_handle = mock.MagicMock()
        mock_local_storage.open.return_value = mock_local_handle
        mock_get_local_storage.return_value = mock_local_storage

        s3_storage._get_local_storage = mock_get_local_storage

        # ensure that the function returns a handle to the LOCAL
        # file as to avoid the time-consuming download multiple times.
        return_val = s3_storage._open(name, mode)
        self.assertEqual(return_val, mock_local_handle)
        mock_local_storage.open.assert_called_with(name, mode)
        mock_local_storage.save.assert_called_with(name, mock_remote_handle)

        # also ensure we actually called the remote open
        mock_S3Boto3Storage_open.assert_called()

    @mock.patch('api.storage.alert_admins')
    @mock.patch('api.storage.S3Boto3Storage._open')
    def test_s3_storage_caching_error_catch(self, mock_S3Boto3Storage_open, \
        mock_alert_admins):
        '''
        Tests that the we alert admins and don't cause any 
        access failures in case local storage cache does not work
        for some reason
        '''
        s3_storage = S3ResourceStorage()

        # Need to mock the _open method of the S3Boto3Storage class
        # since we only want to check that it was called- can't interact
        # with S3 in the test suite
        mock_remote_handle = mock.MagicMock()
        mock_S3Boto3Storage_open.return_value = mock_remote_handle

        mock_get_local_storage = mock.MagicMock()
        mock_local_storage = mock.MagicMock()
        # mock that the file DOES NOT exist in the local storage already
        mock_local_storage.exists.return_value = False

        # mock an issue with the save call:
        mock_local_storage.save.side_effect = Exception('!!!')

        name = 'foo'
        mode = 'abc'

        mock_local_handle = mock.MagicMock()
        mock_get_local_storage.return_value = mock_local_storage

        s3_storage._get_local_storage = mock_get_local_storage

        return_val = s3_storage._open(name, mode)
        self.assertEqual(return_val, mock_remote_handle)
        mock_alert_admins.assert_called()