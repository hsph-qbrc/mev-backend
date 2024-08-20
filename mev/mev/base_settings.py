import os
import logging
from datetime import timedelta

from django.core.exceptions import ImproperlyConfigured
from django.conf import global_settings
from django.utils.module_loading import import_string

from dotenv import load_dotenv

from .settings_helpers import get_env

load_dotenv()

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# A directory where we store user data, etc. Outside of the source tree!
DATA_DIR = get_env('DATA_DIR')

# double-check that the data dir exists:
if not os.path.exists(DATA_DIR):
    raise ImproperlyConfigured('There needs to be a directory located at {d}'
        ' for user and operation data.'.format(d=DATA_DIR)
    )

try:
    db_name = os.environ['DB_NAME']
except KeyError:
    db_name = 'webmev'

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': db_name,
        'USER': get_env('DB_USER'),
        'PASSWORD': get_env('DB_PASSWD'),
        'HOST': get_env('DB_HOST'),
        'PORT': 5432,
    }
}

ALLOWED_HOSTS = [x for x in os.environ.get('DJANGO_ALLOWED_HOSTS', '').split(',') if len(x) > 0]
ALLOWED_HOSTS.append('127.0.0.1')

# Necessary for use of the DRF pages and django 4.0+
CSRF_TRUSTED_ORIGINS = ['https://' + x for x in ALLOWED_HOSTS]

CORS_ORIGIN_ALLOW_ALL = False
CORS_ORIGIN_WHITELIST = [
    x for x in os.environ.get('DJANGO_CORS_ORIGINS', '').split(',') if len(x) > 0
]
CORS_EXPOSE_HEADERS = ['Content-Disposition']

# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    'rest_framework.authtoken',
    'api.apps.ApiConfig',
    'corsheaders',
    'social_django',
    'debug_toolbar',
]

MIDDLEWARE = [
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'mev.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'mev.wsgi.application'


# Password validation
# https://docs.djangoproject.com/en/3.0/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/3.0/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/3.0/howto/static-files/

STATIC_URL = '/static/'
STATIC_ROOT = get_env('STATIC_ROOT')

# use an alternate user model which has the email as the username
AUTH_USER_MODEL = 'api.CustomUser'

# This suppresses warnings for models where an explicit
# primary key is not defined.
DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'

# The location where we will dump the testing database
TESTING_DB_DUMP = os.path.join(BASE_DIR, 'api', 'tests', 'test_db.json')


REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': [
        'rest_framework_simplejwt.authentication.JWTAuthentication',
        'rest_framework.authentication.SessionAuthentication',
    ],
    'DEFAULT_PERMISSION_CLASSES': [
        'rest_framework.permissions.IsAuthenticated',
    ],
    'PAGE_SIZE': 50,
    'DEFAULT_PAGINATION_CLASS': None
}
# This silences the page_size and default_pagination_class warning
# that happens on startup.
# We only paginate api requests if someone explicitly uses a 
# ?page=X query param (e.g. from api/resources which can be quite large). 
# However, we want to set some page_size default.
# If we set DEFAULT_PAGINATION_CLASS to something other than
# None, then the api defaults to responding with paginated payloads,
# which is cumbersome for the frontend framework since the data is 
# nested
SILENCED_SYSTEM_CHECKS = ["rest_framework.W001"]

# settings for the DRF JWT app:
SIMPLE_JWT = {
    'USER_ID_FIELD': 'user_uuid',
    'ACCESS_TOKEN_LIFETIME': timedelta(minutes=30)
}


###############################################################################
# Parameters for Email functions
###############################################################################

FROM_EMAIL = get_env('FROM_EMAIL')

ADMIN_EMAIL_LIST = [x for x in os.environ.get('ADMIN_EMAIL_CSV', '').split(',') if len(x) > 0]
###############################################################################
# END Parameters for Email functions
###############################################################################

# The location of the mkdocs YAML configuration file
MAIN_DOC_YAML = os.path.join(BASE_DIR, 'api', 'docs', 'mkdocs.yml')


# A directory where we temporarily write uploads send to the server
# After completion, they are moved to a users' own storage based 
# on the storage backend
PENDING_UPLOADS_DIR = os.path.join(DATA_DIR, 'pending_user_uploads')
if not os.path.exists(PENDING_UPLOADS_DIR):
    raise ImproperlyConfigured('Please create a directory for the'
    ' pending uploaded files at {path}.'.format(
        path = PENDING_UPLOADS_DIR)
    )

# A tmp dir where we place files that are in the process of being validated,
# created, or manipulated. Nothing permanent there.
# We perform the work there so as not to potentially corrupt the "real"
# user files in our local cache.
TMP_DIR = os.path.join(DATA_DIR, 'tmp')
if not os.path.exists(TMP_DIR):
    raise ImproperlyConfigured('Please create a directory for'
        f' resource validation at {TMP_DIR}.')

# change the class that handles the direct file uploads.  This provides a mechanism
# to query for upload progress.
FILE_UPLOAD_HANDLERS = ['mev.upload_handler.UploadProgressCachedHandler',] + \
    global_settings.FILE_UPLOAD_HANDLERS


###############################################################################
# Parameters for domains and front-end URLs
###############################################################################

# For some of the auth views, various links (sent via email) such as for account
# activation, will direct users to the front-end.  There, the front-end will 
# grab the important components like the token, and send them to the backend, hitting
# the usual API endpoints.

FRONTEND_DOMAIN = get_env('FRONTEND_DOMAIN')
BACKEND_DOMAIN = get_env('BACKEND_DOMAIN')
SITE_NAME = 'WebMeV'

# Note that the leading "#" is used for setting up the route
# in the front-end correctly.
ACTIVATION_URL = 'activate/{uid}/{token}'
RESET_PASSWORD_URL = 'reset-password/{uid}/{token}'


###############################################################################
# END Parameters for domains and front-end URLs
###############################################################################


###############################################################################
# START Parameters for configuring the cloud environment
###############################################################################

# For consistent reference, define the cloud platforms
AMAZON = 'aws'
VIRTUALBOX = 'virtualbox'

# include any cloud platforms that are implemented in this list.
AVAILABLE_WEBMEV_DEPLOYMENT_PLATFORMS = [AMAZON, VIRTUALBOX]

# get the requested platform from the environment variables and ensure 
# that it's valid
WEBMEV_DEPLOYMENT_PLATFORM = get_env('WEBMEV_DEPLOYMENT_PLATFORM')
if not WEBMEV_DEPLOYMENT_PLATFORM in AVAILABLE_WEBMEV_DEPLOYMENT_PLATFORMS:
    raise ImproperlyConfigured('Requesting a platform'
        f' ({WEBMEV_DEPLOYMENT_PLATFORM}) that is not implemented.'
        f' Please choose from:'
        f' {", ".join(AVAILABLE_WEBMEV_DEPLOYMENT_PLATFORMS)}'
    )

if get_env('ENABLE_REMOTE_JOB_RUNNERS') == 'yes':
    ENABLE_REMOTE_JOBS = True

    JOB_BUCKET_NAME = get_env('JOB_BUCKET_NAME')
    AWS_BATCH_QUEUE = get_env('AWS_BATCH_QUEUE')
    AWS_REGION = get_env('AWS_REGION')

    # TODO: make these variable:
    NEXTFLOW_EXE = '/opt/nextflow'
    NEXTFLOW_STATUS_UPDATE_URL = 'http://127.0.0.1:8080/api/nextflow/status-update/'

    # variables related to ECS-based runner:
    AWS_ECS_CLUSTER = get_env('AWS_ECS_CLUSTER')
    AWS_ECS_EXECUTION_ROLE = get_env('AWS_ECS_EXECUTION_ROLE')
    AWS_ECS_SECURITY_GROUP = get_env('AWS_ECS_SECURITY_GROUP')
    AWS_ECS_SUBNET = get_env('AWS_ECS_SUBNET')
    AWS_ECS_TASK_ROLE = get_env('AWS_ECS_TASK_ROLE')
    AWS_EFS_ACCESS_POINT = get_env('AWS_EFS_ACCESS_POINT')
    AWS_EFS_ID = get_env('AWS_EFS_ID')
    AWS_LOG_GROUP = get_env('CLOUDWATCH_LOG_GROUP')

else:
    ENABLE_REMOTE_JOBS = False

###############################################################################
# END Parameters for configuring the cloud environment
###############################################################################


###############################################################################
# START Parameters for configuring resource storage
###############################################################################

LOCAL = 'local'
REMOTE = 'remote'

# a directory (relative to the storage root) where we copy resources that fail
# validation. This permits us to check files that failed validation in case
# the user deletes the file before an admin can check
DEBUG_DIR = 'debug'

# map to the implementing classes. For the remote jobs, we have to reference
# the cloud environment to get the implementing class.
# For each cloud environment, we only allow certain storage backends. For example,
# if we are on GCP, we don't allow AWS S3 storage backend (for simplicity)
# Additionally, if we have chosen to use remote job runners, we will REQUIRE
# that the corresponding bucket storage is used for the backend
AVAILABLE_STORAGE_BACKENDS = [LOCAL, REMOTE]
STORAGE_LOCATION = get_env('STORAGE_LOCATION')
if not (STORAGE_LOCATION in AVAILABLE_STORAGE_BACKENDS):
    raise ImproperlyConfigured('The STORAGE_LOCATION environment'
        ' variable must be one of: {opts}'.format(
            opts = ', '.join([LOCAL, REMOTE])
        )
    )

if ENABLE_REMOTE_JOBS and (STORAGE_LOCATION==LOCAL):
    raise ImproperlyConfigured('Since you enabled remote jobs, you must choose'
        ' remote storage. Edit your environment variables.'
    )

# In the case of remote storage backends (e.g. buckets), we want the ability
# to locally cache the files for faster access.  Files in this directory
# are temporary and will be removed after some period of inactivity
RESOURCE_CACHE_DIR = os.path.join(DATA_DIR, 'resource_cache')
if not os.path.exists(RESOURCE_CACHE_DIR):
    raise ImproperlyConfigured('There should be a directory at {d}.'.format(
            d = RESOURCE_CACHE_DIR
        )
    )


# How long should the files be kept in the local cache.
# Check the functions for periodic tasks to see how this
# parameter is used.
RESOURCE_CACHE_EXPIRATION_DAYS = 2

# The maximum size (in bytes) to allow "direct" downloads from the API.
# If the file exceeds this, we ask the user to download in another way. 
# Most files are small and this will be fine. However, we don't want users
# trying to download BAM or other large files. They can do that with other methods,
# like via Dropbox.
MAX_DOWNLOAD_SIZE_BYTES = 512 * 1000 * 1000

if STORAGE_LOCATION == REMOTE:
    if WEBMEV_DEPLOYMENT_PLATFORM == AMAZON:
        FILE_STORAGE_BACKEND = 'api.storage.S3ResourceStorage'
        # note that django storages makes use of these settings:
        AWS_S3_SIGNATURE_VERSION = 's3v4'
        AWS_S3_REGION_NAME = get_env('AWS_REGION')
    else:
        raise NotImplementedError('Storage not implemented for'
                                  f' platform: {WEBMEV_DEPLOYMENT_PLATFORM}')

    # Regardless of the platform, we still need to know the bucket name.
    # This setting is used by the storage class implementation to effectively
    # set the media root
    MEDIA_ROOT = get_env('STORAGE_BUCKET_NAME')
else: # local storage
    # We extend the django native django.core.files.storage.FileSystemStorage
    # so that we can implement methods which avoid extra conditionals.
    # An example would be localization of files for use in Docker containers.
    # Rather than checking to see if storage is local or remote, we provide
    # a "dumb" localization method
    FILE_STORAGE_BACKEND = 'api.storage.LocalResourceStorage'
    MEDIA_ROOT = RESOURCE_CACHE_DIR

STORAGES = {
    "default": {
        "BACKEND": FILE_STORAGE_BACKEND,
    },
    "local": {
        "BACKEND": "api.storage.LocalResourceStorage"
    },
    "staticfiles": {
        "BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage",
    },
}
###############################################################################
# END Parameters for configuring resource storage
###############################################################################

###############################################################################
# START Parameters for configuring social authentication/registration
###############################################################################

AUTHENTICATION_BACKENDS = [
    'social_core.backends.google.GoogleOAuth2',
    # required for usual username/password authentication
    'django.contrib.auth.backends.ModelBackend',
]

SOCIAL_AUTH_STRATEGY = 'api.social_auth_strategy.WebMeVAuthStrategy'

# sets the proper redirect URL (e.g. <FRONTEND_URL>/redirect/)
# which is the frontend client
REST_SOCIAL_OAUTH_REDIRECT_URI = '/oauth2-redirect/'

# This is the default, but we are being explicit here that the redirect
# URL should correspond to the frontend.
REST_SOCIAL_DOMAIN_FROM_ORIGIN = True

SOCIAL_AUTH_GOOGLE_OAUTH2_KEY = get_env('GOOGLE_OAUTH2_CLIENT_ID')
SOCIAL_AUTH_GOOGLE_OAUTH2_SECRET = get_env('GOOGLE_OAUTH2_CLIENT_SECRET')

###############################################################################
# END Parameters for configuring social authentication/registration
###############################################################################

# For remote services (like checking auth with google), how many 
# times should be attempt to communicate before failing:
MAX_RETRIES = 3


###############################################################################
# START Hooks/setup for Sentry (if used)
###############################################################################

USING_SENTRY = False # by default, do NOT enable sentry
SENTRY_URL = get_env('SENTRY_URL')

if ( (len(SENTRY_URL) > 0) & (SENTRY_URL.startswith('http')) ):
    import sentry_sdk
    from sentry_sdk.integrations.django import DjangoIntegration
    from sentry_sdk.integrations.celery import CeleryIntegration

    sentry_sdk.init(
        dsn=SENTRY_URL,
        integrations=[DjangoIntegration(), CeleryIntegration()],

        # If you wish to associate users to errors (assuming you are using
        # django.contrib.auth) you may enable sending PII data.
        send_default_pii=True
    )

    USING_SENTRY = True

###############################################################################
# END Hooks/setup for Sentry (if used)
###############################################################################


###############################################################################
# START Settings for ingestion of Operations
###############################################################################

# the name of a directory where new Operation specifications will be cloned
# and staged for ingestion
CLONE_STAGING_DIR = os.path.join(DATA_DIR, 'operation_staging')
if not os.path.exists(CLONE_STAGING_DIR):
    raise ImproperlyConfigured('There should be a directory at {d} for staging'
        ' new operations.'.format(
            d = CLONE_STAGING_DIR
        )
    )
# the name of the file that contains the specification for an Operation:
OPERATION_SPEC_FILENAME = 'operation_spec.json'

# a local directory where the various Operations are stashed
OPERATION_LIBRARY_DIR = os.path.join(DATA_DIR, 'operations')
if not os.path.exists(OPERATION_LIBRARY_DIR):
    raise ImproperlyConfigured('There should be a directory at {d} for preserving'
        ' operation files'.format(
            d = OPERATION_LIBRARY_DIR
        )
    )
# This is a list of domains from which we will pull repositories
# If the domains are not in this list, then we will block any attempts
# to clone repositories.
ACCEPTABLE_REPOSITORY_DOMAINS = ['github.com',]

###############################################################################
# END Settings for ingestion of Operations
###############################################################################

###############################################################################
# START Settings for Operation executions
###############################################################################

# a directory where the operations will be run-- each execution of an operation
# gets its own sandbox
OPERATION_EXECUTION_DIR = os.path.join(DATA_DIR, 'operation_executions')
if not os.path.exists(OPERATION_EXECUTION_DIR):
    raise ImproperlyConfigured('There should be a directory at {d}.'.format(
            d = OPERATION_EXECUTION_DIR
        )
    )

# To check on the status of both local and remote-based jobs, we have a celery-
# based task that polls for status. This sets how frequently this happens
JOB_STATUS_CHECK_INTERVAL = 3 # seconds

###############################################################################
# END Settings for Operation executions
###############################################################################


###############################################################################
# START Settings for Docker container repos
###############################################################################

# Some explanation for the items below:
# The CONTAINER_REGISTRY defines where we pull the Docker images from. For example,
# we can pull from the github container repos or Dockerhub. 
# Beyond just the repo, we have the concept of an "organization". This roughly
# corresponds to "accounts" at those container registries. After all, a fully
# qualified Docker url is something like ghcr.io/<org>/<img>:<tag> (e.g. for
# github) or docker.io/<org>/<img>:<tag> for Dockerhub.

# A string indicating where the Docker containers are held. For available
# options, see api.container_registries.__init__.py 
CONTAINER_REGISTRY = get_env('CONTAINER_REGISTRY')

# A string that indicates where Docker containers are held. 
DOCKER_REPO_ORG = 'web-mev'

###############################################################################
# END Settings for Docker container repos
###############################################################################


###############################################################################
# START Settings for public datasets
###############################################################################

PUBLIC_DATA_DIR = os.path.join(DATA_DIR, 'public_data')
if not os.path.exists(PUBLIC_DATA_DIR):
    raise ImproperlyConfigured('There should be a directory for public data at {d}.'.format(
            d = PUBLIC_DATA_DIR
        )
    )

# name of a bucket from which we can pull public datasets
PUBLIC_DATA_BUCKET_NAME = get_env('PUBLIC_DATA_BUCKET_NAME')

###############################################################################
# END Settings for public datasets
###############################################################################

###############################################################################
# START settings/imports for filtering
###############################################################################

# For pagination-- sets up a consistent reference.
PAGE_PARAM = 'page'
PAGE_SIZE_PARAM = 'page_size'

# import all the filtering operations that can be applied when querying for the content
# of data resources
from api.filters import *

###############################################################################
# END settings/imports for filtering
###############################################################################


###############################################################################
# START settings/imports for public data indexing
###############################################################################

# This determines which indexer we are using for the public data.
# See api/public_data/indexers/__init__.py for the implementing classes.
# Specifically, choose one of the keys in INDEXER_CHOICES
# Don't change unless you know what you're doing.
PUBLIC_DATA_INDEXER = 'solr'

###############################################################################
# END settings/imports for public data indexing
###############################################################################


###############################################################################
# START settings for Globus
###############################################################################

# Some general settings for Globus:

# the (frontend) URL where Globus will redirect for the OAuth2 exchange.
# The view using this will detect the origin and fill the templated string
GLOBUS_AUTH_REDIRECT_URI = '{origin}/globus/auth-redirect/'

# the (frontend) URL where the Globus file chooser will redirect to once
# the files are chosen.
GLOBUS_UPLOAD_REDIRECT_URI = '{origin}/globus/upload-redirect/'
GLOBUS_DOWNLOAD_REDIRECT_URI = '{origin}/globus/download-redirect/'
GLOBUS_UPLOAD_CALLBACK_METHOD = 'GET'
GLOBUS_BROWSER_UPLOAD_URI = 'https://app.globus.org/file-manager?action={callback}&method=GET'
GLOBUS_BROWSER_DOWNLOAD_URI = 'https://app.globus.org/file-manager?action={callback}&method=GET&folderlimit=1&filelimit=0'

GLOBUS_TRANSFER_SCOPE = 'urn:globus:auth:scope:transfer.api.globus.org:all'
GLOBUS_SCOPES = (
    "openid",
    "profile",
    "email",
    GLOBUS_TRANSFER_SCOPE,
)
GLOBUS_REAUTHENTICATION_WINDOW_IN_MINUTES = 60

try:
    # this is the client/secret for the endpoint manager.
    GLOBUS_ENDPOINT_CLIENT_ID = get_env('GLOBUS_ENDPOINT_CLIENT_UUID')
    GLOBUS_ENDPOINT_CLIENT_SECRET = get_env('GLOBUS_ENDPOINT_CLIENT_SECRET')

    # this is the client/secret for the application, NOT for the
    # Globus endoint
    GLOBUS_CLIENT_ID = get_env('GLOBUS_APP_CLIENT_ID')
    GLOBUS_CLIENT_SECRET = get_env('GLOBUS_APP_CLIENT_SECRET')

    # This endpoint ID is the UUID of the shared collection,
    # NOT the endpoint ID of the GCS
    GLOBUS_ENDPOINT_ID = get_env('GLOBUS_ENDPOINT_ID')

    # The bucket where Globus will place files. Globus does NOT
    # have access to the WebMeV buckets
    GLOBUS_BUCKET = get_env('GLOBUS_BUCKET_NAME')

    # If those succeeded, then we enable Globus
    GLOBUS_ENABLED = True
except ImproperlyConfigured as ex:
    print(ex)
    GLOBUS_ENABLED = False

###############################################################################
# END settings for Globus
###############################################################################


# Change the LOGLEVEL env variable if you want logging
# different than INFO:
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()

# By default, use a console logger. Override/modify/etc.
# in settings_dev or settings_production modules
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {module}: {message}',
            'style': '{',
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'verbose'
        },
    },
    'root': {
        'handlers': ['console'],
        'level':  LOGLEVEL,
    },
}
