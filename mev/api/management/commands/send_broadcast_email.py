import logging
import time

from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.core.management.base import BaseCommand

from api.models import CustomUser

logger = logging.getLogger(__name__)

# Set these to be in compliance with email rate limits:
BATCH_SIZE = 10
SLEEP_TIME = 2 # in seconds


class Command(BaseCommand):
    help = ('Sends a broadcast email to the entire WebMeV userbase.'
            ' Expects a fully prepared text and html file -'
            ' does NOT perform Jinja interpolation!')

    def add_arguments(self, parser):

        parser.add_argument(
            '-m',
            '--message',
            required=True,
            help='Path to the plaintext message.'
        )

        parser.add_argument(
            '-p',
            '--html',
            required=True,
            help='Path to the HTML message.'
        )

        parser.add_argument(
            '-s',
            '--subject',
            required=True,
            help='The subject of the email.'
        )
        
    def handle(self, *args, **options):

        # Email addresses for all the active users in our database
        emails = [x.email for x in CustomUser.objects.filter(is_active=True)]

        with open(options['message'], 'r') as fin:
            text_content = fin.read()
        with open(options['html'], 'r') as fin:
            html_content = fin.read()

        # Due to limits in the number of requests, we need to be careful of our rate.
        # Also, since this is a broadcast email, we can't put multiple people in the TO
        # since they can see each other. Here, we make small batches in the BCC where the
        # batch size and sleep periods are setup to be in complianace with our 
        # mail server
        n_batches = 1 + len(emails) // BATCH_SIZE
        for i in range(n_batches):
            logger.info(f'Send batch {i}')
            start = i * BATCH_SIZE
            end = (i + 1) * BATCH_SIZE
            email_chunk = emails[start:end]
            message = EmailMultiAlternatives(
                options['subject'], 
                text_content, 
                settings.FROM_EMAIL,
                [],
                email_chunk)
            message.attach_alternative(html_content, 'text/html')
            message.send()
            time.sleep(SLEEP_TIME)
