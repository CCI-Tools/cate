import unittest

import os
import shutil
import signal
import tempfile

from cate.util.web.serviceinfo import read_service_info
from cate.util.web.webapi import find_free_port, WebAPI
from cate.webapi.wsmanag import WebAPIWorkspaceManager
from tests.core.test_wsmanag import WorkspaceManagerTestMixin

_SERVICE_INFO_FILE = 'pytest-service-info.json'


@unittest.skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
class WebAPIWorkspaceManagerTest(WorkspaceManagerTestMixin, unittest.TestCase):

    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self._is_relative = False
        self._path_manager = None

    def setUp(self):
        self._root_dir = tempfile.mkdtemp()
        os.environ['CATE_WORKSPACE_ROOT'] = self._root_dir
        self.port = find_free_port()
        WebAPI.start_subprocess('cate.webapi.start',
                                port=self.port,
                                caller='pytest',
                                service_info_file=_SERVICE_INFO_FILE)

    def tearDown(self):
        del os.environ['CATE_WORKSPACE_ROOT']
        shutil.rmtree(self._root_dir)
        service_info = read_service_info(_SERVICE_INFO_FILE)
        if service_info:
            os.kill(service_info['pid'], signal.SIGTERM)
        else:
            print("WebAPIWorkspaceManagerTest: error: can't find %s" % _SERVICE_INFO_FILE)

    def new_workspace_manager(self):
        return WebAPIWorkspaceManager(dict(port=self.port), rpc_timeout=2)
