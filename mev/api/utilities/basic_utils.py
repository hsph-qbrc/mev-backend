import os
import shutil
import errno
import logging
import requests
import backoff
import subprocess as sp
import shlex
import hashlib

from django.conf import settings
from django.utils.encoding import force_bytes, force_str
from django.utils.http import urlsafe_base64_decode, urlsafe_base64_encode

logger = logging.getLogger(__name__)

def is_fatal_code(e):
    if e.response:
        return 400 <= e.response.status_code < 500
    else:
        # if e.response is None, then the most likely situation is
        # that the url was wrong or unresponsive.
        # Return True to indicate a fatal problem.
        return True

# a function that wraps requests.get for multiple tries
@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_time=30,
                      max_tries = 5,
                      giveup=is_fatal_code)
def get_with_retry(*args, **kwargs):
    return requests.get(*args, **kwargs)

# a function that wraps requests.get for multiple tries
@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_time=30,
                      max_tries = 5,
                      giveup=is_fatal_code)
def post_with_retry(*args, **kwargs):
    return requests.post(*args, **kwargs)

def encode_uid(pk):
    return force_str(urlsafe_base64_encode(force_bytes(pk)))

def decode_uid(pk):
    return force_str(urlsafe_base64_decode(pk))

def make_local_directory(directory_path):
    '''
    A single location where we create directories and handle failures
    (hopefully) gracefully
    '''
    try:
        logger.info('Attempt to create directory at %s' % directory_path)
        os.makedirs(directory_path)
    except FileExistsError as ex:
        if ex.errno == errno.EEXIST:
            # Should not happen since
            # we previously checked that it did not exist.
            # However, not REALLY an error (could have been
            # caused by a race condition)
            logger.warning('Directory at %s already existed, but previously was'
                ' determined not to exist.  Possible result of race condition when'
                ' creating directories for the same user.' % directory_path
            )
            return 
        else:
            logger.error('A FileExistsError was raised when trying to create a'
                ' directory at %s.  However, the issue was not a simple case'
                ' where it already existed.  Error number was %d.' % (directory_path,
                ex.errno)
            )
            raise ex
    except PermissionError as ex:
        if ex.errno == errno.EACCES:
            # could not create the directory due to permission issue.
            # Should not happen.  
            logger.error('Could not create a directory at %s'
                ' due to permissions issues' % directory_path
            )
        raise ex

def move_resource(source, dest):
    '''
    Handles relocation of files from source to dest.
    Essentially a 'mv' with error checking.
    '''

    logger.info('Moving resource from %s to %s' % (source, dest))

    # in the trivial case of a "self move", immediately return.
    if source == dest:
        logger.info('Trivial move requested. Do nothing.')
        return dest

    # check that the source actually exists:
    if not os.path.exists(source):
        logger.error('The file at %s did not exist.  This likely means'
        ' the database was corrupted.') 
        raise FileNotFoundError('No file exists at %s' % source)

    # check that the file does not already exist.
    # Since we prefix with a UUID, this is VERY unlikely
    # but we still check.
    # Given the fact that it's nigh impossible, we simply
    # pre-pend an integer until it becomes unique.
    # e.g. if /a/b.txt exists, we try /a/0b.txt.
    # if THAT exists, we try /a/10b.txt, etc.
    i = 0
    while os.path.exists(dest):
        logger.info('There was already a file at %s.'
            ' Changing the destination filename.' % dest)
        b = os.path.basename(dest)
        d = os.path.dirname(dest)
        b = '%d%s' % (i,b)
        dest = '%s/%s' % (d, b)
        i += 1

    try:
        logger.info('Moving from %s to %s' % (source, dest))
        shutil.move(source, dest)
        return dest
    except FileExistsError as ex:
        if ex.errno == errno.EEXIST:
            directory_listing = os.listdir(os.path.dirname(dest))
            logger.error('File existed at %s, despite just checking this.'
                ' Directory listing is: %s' % (dest, '\n'.join(directory_listing))
            ) 
        else:
            logger.error('Some other FileExistsError (%s) was raised'
                ' when moving the file.' % (ex.errno)) 

        # regardless, raise the exception to catch higher-up
        raise ex
    except PermissionError as ex:
        if ex.errno == errno.EACCES:
            # could not move the file due to permission issue.
            # Should not happen.  
            logger.error('Could not move file due to permissions issue.')
        raise ex

def copy_local_resource(src, dest):
    '''
    Wraps the basic shutil copyfile. 
    '''
    logger.info('Copying from {src} to {dest}'.format(
        src=src,
        dest=dest
    ))
    try:
        final_dest = shutil.copyfile(src, dest)
        logger.info('Success in copy from {src} to {dest}'.format(
            src=src,
            dest=dest
        ))
        return final_dest
    except OSError as ex:
        logger.error('Experienced an OSError when copying'
        ' from {src} to {dest}'.format(
            src=src,
            dest=dest
        ))
        raise ex
    except shutil.SameFileError as ex:
        logger.error('shutil.copyfile raised a SameFileError'
        ' exception when copying from {src} to {dest}'.format(
            src=src,
            dest=dest
        ))
    except Exception as ex:
        logger.error('Caught an unhandled exception.  Was {err}'.format(err=str(ex)))
        raise ex
    

def delete_local_file(path):
    '''
    Deletes a local file.
    '''
    logger.info('Requesting deletion of {path}'.format(path=path))
    try:
        os.remove(path)
        logger.info('Success in removing {path}'.format(path=path))
    except FileNotFoundError as ex:
        logger.error('Tried to remove a Resource path that'
            ' pointed at a non-existent file: {path}'.format(path=path))
    except IsADirectoryError as ex:
        logger.error('Tried to remove a Resource path that'
            ' pointed at a directory: {path}'.format(path=path))
        raise ex
    except Exception as ex:
        logger.error('General exception handled.'
            'Could not delete the file at {path}'.format(path=path))
        raise ex

def read_local_file(filepath, mode='r'):
    '''
    Reads a local file, returning a file handle.

    Basically this wraps the file read with logging
    and error-catching.
    '''
    try:
        logger.info('Read file at {path}'.format(
            path=filepath
        ))
        return open(filepath, mode)
    except Exception as ex:
        logger.error('Could not read file at {path}. Exception was {ex}'.format(
            path = filepath,
            ex = ex
        ))
        raise ex

def recursive_copy(src, dest, include_hidden=False, overwrite=False):
    '''
    Performs a recursive copy from src to dest

    Simply adds logging to basic python utils.

    By default, this skips "hidden" directories (defined
    as those that begin with a dot ".").
    '''
    logger.info('Perform a recursive copy from {src}-->{dest}'.format(
        src=src,
        dest=dest
    ))

    # the shutil method used below does not natively allow for overwrite
    # so if we specifically request overwrite, we have to first remove
    # the directory
    if os.path.exists(dest):
        if overwrite:
            logger.info('The destination directory ({d}) existed, but we want to force overwrite.'
                ' Remove the directory.'.format(d=dest)
            )
            shutil.rmtree(dest)
        else: # dir exists and we do NOT want to overwrite
            logger.info('The destination directory ({d}) existed, but we did not specifically'
                ' request overwrite.'.format(d=dest)
            )
            raise Exception('Attempted to recursively copy a directory tree, but it already existed'
                ' and an overwrite was not specifically forced.'
            )


    def skip_hidden(dir, listing):
        ret = []
        for x in listing:
            if x.startswith('.'):
                ret.append(x)
        return ret 

    if not include_hidden:
        logger.info('Will skip hidden files.')
        shutil.copytree(src, dest, ignore=skip_hidden)
    else:
        logger.info('Copying all, including hidden files.')
        shutil.copytree(src, dest)

def run_shell_command(cmd):
    '''
    Wrapper around the basic Popen command to add logging.

    `cmd` is a single string command, as one might run in a bash shell
    '''
    logger.info('Run shell command: {cmd}'.format(cmd=cmd))
    split_cmd = shlex.split(cmd)

    p = sp.Popen(split_cmd, stdout=sp.PIPE, stderr=sp.STDOUT)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        logger.error('Problem with running the command:'
            ' {cmd}. STDERR was: {stderr}\nSTDOUT was: {stdout}'.format(
                cmd=cmd,
                stderr=stderr,
                stdout=stdout
            )
        )
        raise Exception('Failed when executing command. See logs.')
    else:
        return stdout, stderr
    