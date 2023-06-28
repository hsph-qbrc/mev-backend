from django.db import models


class Message(models.Model):
    message = models.CharField(max_length=1000)
    creation_datetime=models.DateTimeField(
        auto_now_add=True
    )