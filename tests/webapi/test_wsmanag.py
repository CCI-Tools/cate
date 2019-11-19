import os
import signal
import unittest

from cate.util.web.serviceinfo import read_service_info
from cate.util.web.webapi import find_free_port, WebAPI
from cate.webapi.wsmanag import WebAPIWorkspaceManager
from tests.core.test_wsmanag import WorkspaceManagerTestMixin

_SERVICE_INFO_FILE = 'pytest-service-info.json'


@unittest.skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
class WebAPIWorkspaceManagerTest(WorkspaceManagerTestMixin, unittest.TestCase):
    def setUp(self):
        self.port = find_free_port()
        WebAPI.start_subprocess('cate.webapi.start',
                                port=self.port,
                                caller='pytest',
                                service_info_file=_SERVICE_INFO_FILE)

    def tearDown(self):
        service_info = read_service_info(_SERVICE_INFO_FILE)
        if service_info:
            os.kill(service_info['pid'], signal.SIGTERM)
        else:
            print("WebAPIWorkspaceManagerTest: error: can't find %s" % _SERVICE_INFO_FILE)

    def new_workspace_manager(self):
        return WebAPIWorkspaceManager(dict(port=self.port), rpc_timeout=2)
