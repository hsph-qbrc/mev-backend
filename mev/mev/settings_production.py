import os
import copy
import logging.config

from django.core.exceptions import ImproperlyConfigured

from .base_settings import * 

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = get_env('DJANGO_SECRET_KEY')

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = False

###############################################################################
# START Check for production-specific settings/params
###############################################################################
if EMAIL_BACKEND_CHOICE == 'CONSOLE':
    raise ImproperlyConfigured('In production you cannot use the console email'
        ' backend, as it does not actually send email!'
)
###############################################################################
# END Check for production-specific settings/params
###############################################################################




###############################################################################
# START logging settings
###############################################################################

# setup some logging options for production:
production_config_dict = copy.deepcopy(log_config.base_logging_config_dict)

# make changes to the default logging dict here:

# finally, register this config:
logging.config.dictConfig(production_config_dict)

###############################################################################
# END logging settings
###############################################################################
