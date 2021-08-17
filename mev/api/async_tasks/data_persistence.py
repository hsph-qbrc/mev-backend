import logging

from celery.decorators import task

logger = logging.getLogger(__name__)

@task(name='persist_local_data')
def persist_local_data(path):
    '''
    Persists local data (like operations, executed operations) to
    a bucket. Only executed if we are using a remote storage backend.
    '''
    logger.info('Requesting deletion of {path}'.format(path=path))
    print('in data persistence, arg=', path)