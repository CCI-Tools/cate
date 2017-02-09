import os
import sys
import time
import unittest

from cate.util.web.webapi import find_free_port, WebAPI
from cate.webapi.wsmanag import WebAPIWorkspaceManager
from test.core.test_wsmanag import WorkspaceManagerTestMixin


@unittest.skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
class WebAPIWorkspaceManagerTest(WorkspaceManagerTestMixin, unittest.TestCase):
    def setUp(self):
        self.port = find_free_port()
        WebAPI.start_subprocess('cate.webapi.main', port=self.port, caller='pytest')

    def tearDown(self):
        WebAPI.stop_subprocess('cate.webapi.main', port=self.port, caller='pytest')
        if sys.platform == 'win32':
            # This helps getting around silly error raised inside Popen._internal_poll():
            # OSError: [WinError 6] Das Handle ist ungültig
            time.sleep(5)

    def new_workspace_manager(self):
        return WebAPIWorkspaceManager(dict(port=self.port), timeout=2)
