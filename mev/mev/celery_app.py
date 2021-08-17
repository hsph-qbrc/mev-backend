import os
from celery import Celery
from django.conf import settings
from django.apps import apps

from celery.schedules import crontab

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'mev.settings')

app = Celery('mev')

# Using a string here means the worker will not have to
# pickle the object when using Windows.
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks(
    lambda: [n.name for n in apps.get_app_configs()],
    related_name = 'async_tasks'
)

# For cron jobs like cleanup, etc

# For jobs that run once per day, at midnight
once_per_day_cronjob = crontab(minute=0, hour=0)
once_per_minute_cronjob = crontab()

app.conf.beat_schedule = {
    'persist_local_data': {
        'task': 'persist_local_data',
        'schedule': once_per_minute_cronjob,
        'args': ('Arg to the real test.',)
    },
    'dummy_periodic_task': {
        'task': 'mev.celery_app.test_periodic',
        'schedule': once_per_minute_cronjob,
        'args': ('Arg to the dummy test.',)
    }
}

@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))

@app.task
def test_periodic(arg):
    print('Dummy periodic task received:', arg)