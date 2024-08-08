import logging.config

import boto3

from .base_settings import * 

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = get_env('DJANGO_SECRET_KEY')

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = False

EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
EMAIL_HOST = os.environ['EMAIL_HOST']
EMAIL_HOST_USER = os.environ['EMAIL_HOST_USER']
EMAIL_HOST_PASSWORD = os.environ['EMAIL_HOST_PASSWORD']
EMAIL_PORT = 587
EMAIL_USE_TLS = True

###############################################################################
# START logging settings
###############################################################################

# By default (in base_settings.py), we set up console logging via
# the `LOGGING` dictionary. Modify below:

if WEBMEV_DEPLOYMENT_PLATFORM == AMAZON:
    boto3_logs_client = boto3.client("logs", region_name=get_env('AWS_REGION'))
    watchtower_params = {
        'class': 'watchtower.CloudWatchLogHandler',
        'boto3_client': boto3_logs_client,
        'log_group_name': get_env('CLOUDWATCH_LOG_GROUP'),
        'level': LOGLEVEL
    }

    LOGGING['root']['handlers'].append('watchtower')
    LOGGING['handlers'].update({'watchtower': watchtower_params})

# finally, register this config:
logging.config.dictConfig(LOGGING)

###############################################################################
# END logging settings
###############################################################################
