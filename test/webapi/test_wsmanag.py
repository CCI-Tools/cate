import os
import sys
import time
import unittest

from cate.core.wsmanag import WebAPIWorkspaceManager
from cate.util.web.webapi import find_free_port, start_service_subprocess, stop_service_subprocess
from test.core.test_wsmanag import WorkspaceManagerTestMixin

NETCDF_TEST_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'precip_and_temp.nc')


@unittest.skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
class WebAPIWorkspaceManagerTest(WorkspaceManagerTestMixin, unittest.TestCase):
    def setUp(self):
        self.port = find_free_port()
        start_service_subprocess('cate.webapi.main', port=self.port, caller='pytest')

    def tearDown(self):
        stop_service_subprocess('cate.webapi.main', port=self.port, caller='pytest')
        if sys.platform == 'win32':
            # This helps getting around silly error raised inside Popen._internal_poll():
            # OSError: [WinError 6] Das Handle ist ungültig
            time.sleep(5)

    def new_workspace_manager(self):
        return WebAPIWorkspaceManager(dict(port=self.port), timeout=2)
