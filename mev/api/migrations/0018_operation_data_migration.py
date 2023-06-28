# Generated by Django 4.1.7 on 2023-06-28 15:22

'''
This is a data migration to populate values in the new fields of the
api.models.Operation database model class
'''

from django.db import migrations

from api.utilities.operations import get_operation_instance_data


def extract_commit_and_repo(apps, schema_editor):
    '''
    Prior to this migration, we kept the git commit and repo
    as items in our operation specification files instead of
    in the database.

    This script uses our utility functions to read those existing
    operations, get those values, and populate the database.
    '''
    Operation = apps.get_model('api', 'Operation')
    for op in Operation.objects.all():
        op_data = get_operation_instance_data(op)
        op.git_commit = op_data['git_hash']
        op.repository_url = op_data['repository_url']
        op.save()


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0017_operation_git_commit_operation_repository_url'),
    ]

    operations = [
        migrations.RunPython(extract_commit_and_repo)
    ]
