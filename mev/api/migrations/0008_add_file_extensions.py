# Generated by Django 3.2.12 on 2022-04-26 17:38

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0007_feedbackmessage'),
    ]

    operations = [
        migrations.AddField(
            model_name='operationresource',
            name='file_extension',
            field=models.CharField(default='', max_length=25),
        ),
        migrations.AddField(
            model_name='resource',
            name='file_extension',
            field=models.CharField(default='', max_length=25),
        ),
    ]
