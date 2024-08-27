import unittest
import unittest.mock as mock
import os
import uuid
import datetime

from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings

from exceptions import OutputConversionException, \
    JobSubmissionException

from api.tests.base import BaseAPITestCase
from api.runners.remote_cromwell import RemoteCromwellRunner
from api.models.operation import Operation
from api.models.executed_operation import ExecutedOperation
from api.models.workspace_executed_operation import WorkspaceExecutedOperation
from api.models.workspace import Workspace

# the api/tests dir
TESTDIR = os.path.dirname(__file__)
TESTDIR = os.path.join(TESTDIR, 'operation_test_files', 'demo_cromwell_workflow')


@override_settings(CROMWELL_SERVER_URL='http://mock-cromwell-server:8080')
@override_settings(CROMWELL_BUCKET_NAME='my-bucket')
class RemoteCromwellRunnerTester(BaseAPITestCase):
    '''
    Note that this test class was implemented since we need to maintain the
    ability to ingest/inspect Cromwell-based jobs, even if the Cromwell
    runner has been deprecated. This way, existing jobs, their inputs,
    and outputs can be read and results can be displayed properly.
    '''

    def test_preparation(self):
        '''
        Tests that the preparation function does nothing.

        To maintain backwards compatability with previously run
        Cromwell-based jobs, we need to permit those operations to
        be properly ingested. This test just asserts that the 
        `prepare_operation` method does not interfere with that.
        '''
        rcr = RemoteCromwellRunner()
        mock_op_dir = '/abc'
        mock_op_db_obj = mock.MagicMock()
        rcr.prepare_operation(mock_op_db_obj, mock_op_dir, 'my-repo', '')

    @mock.patch('api.runners.remote_cromwell.alert_admins')
    def test_run_attempt_fails(self, mock_alert_admins):
        '''
        In the event that an admin leaves a Cromwell-based 
        operation active, this test ensures that the proper 
        actions are taken (including alerting admins).
        '''
        mock_ex_op = mock.MagicMock()
        mock_op = mock.MagicMock()

        rcr = RemoteCromwellRunner()
        rcr.run(mock_ex_op, mock_op, {})
        mock_alert_admins.assert_called()

        self.assertTrue(mock_ex_op.job_failed)
        self.assertTrue(len(mock_ex_op.error_messages) > 0)
        mock_ex_op.save.assert_called()