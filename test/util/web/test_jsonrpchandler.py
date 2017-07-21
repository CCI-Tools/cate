import unittest

from cate.util import Monitor
from cate.util.web.jsonrpchandler import JsonRpcWebSocketHandler, set_debug_web_socket_rpc

set_debug_web_socket_rpc(True)

class ApplicationMock:
    def __init__(self):
        self.ui_methods = {}
        self.ui_modules = {}


class ConnectionMock:
    def set_close_callback(self, cb):
        pass


class WsConnectionMock:
    def write_message(self, message, binary=False):
        pass


class RequestMock:
    def __init__(self):
        self.connection = ConnectionMock()


class TestService:
    def __init__(self, app):
        self.app = app

    def doit1(self, a: int, b: float, c: str):
        return a + b * float(c)

    def doit2(self, a: int, b: float, c: str, monitor=Monitor.NONE):
        with monitor.starting('running', 1):
            res = a + b * float(c)
            monitor.progress(work=1)
            return res


class JsonRpcWebSocketHandlerTest(unittest.TestCase):
    def setUp(self):
        self.handler = JsonRpcWebSocketHandler(ApplicationMock(),
                                               RequestMock(),
                                               lambda app: TestService(app))

    def test_open(self):
        self.assertIsNone(self.handler._service)
        self.handler.open()
        self.assertIsNotNone(self.handler._service)

    def test_open_close(self):
        self.assertIsNone(self.handler._service)
        self.handler.open()
        self.assertIsNotNone(self.handler._service)
        self.handler.close()

    def test_on_close(self):
        self.handler.open()
        self.handler.on_close()
        self.assertIsNone(self.handler._service)

    def test_check_origin(self):
        self.assertTrue(self.handler.check_origin(None))

    def test_on_message(self):
        self.handler.open()
        # Simulate effect of HTTP GET request on tornado.web.RequestHandler
        # which establishes the WebSocket connection
        self.handler.ws_connection = WsConnectionMock()

        # Good messages

        ret = self.handler.on_message('{"id": 1, "method": "doit1", "params": {"a": 2, "b": 4.2, "c": "1.6"}}')
        self.assertIsNone(ret)

        ret = self.handler.on_message('{"id": 2, "method": "doit2", "params": {"a": 2, "b": 4.2, "c": "1.6"}}')
        self.assertIsNone(ret)

        ret = self.handler.on_message('{"id": 3, "method": "__cancel__", "params": {"id": 2}}')
        self.assertIsNone(ret)

        # Bad messages

        ret = self.handler.on_message('{')
        self.assertEqual(ret, 1)

        ret = self.handler.on_message('[]')
        self.assertEqual(ret, 2)

        ret = self.handler.on_message('{"id": null}')
        self.assertEqual(ret, 3)

        ret = self.handler.on_message('{"id": 4, "method": 5}')
        self.assertEqual(ret, 4)

        ret = self.handler.on_message('{"id": 4, "method": ""}')
        self.assertEqual(ret, 4)

        ret = self.handler.on_message('{"id": 4, "method": "__cancel__"}')
        self.assertEqual(ret, 5)

        ret = self.handler.on_message('{"id": 4, "method": "doit3"}')
        self.assertEqual(ret, 6)
