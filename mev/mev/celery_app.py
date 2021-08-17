import os
from celery import Celery
from django.conf import settings
from django.apps import apps

from celery.schedules import crontab

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'mev.settings')

app = Celery('mev')

class Config:
    broker_url = settings.REDIS_BASE_LOCATION
    result_backend = settings.REDIS_BASE_LOCATION
    accept_content = ['json']
    task_serializer = 'json'
    result_serializer = 'json'
    enable_utc = True

# Using a string here means the worker will not have to
# pickle the object when using Windows.
app.config_from_object(Config)
app.autodiscover_tasks(
    lambda: [n.name for n in apps.get_app_configs()],
    related_name = 'async_tasks'
)

# For cron jobs like cleanup, polling for jobs
app.conf.beat_schedule = {}

@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))