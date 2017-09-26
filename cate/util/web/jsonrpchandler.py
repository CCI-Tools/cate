# The MIT License (MIT)
# Copyright (c) 2016, 2017 by the ESA CCI Toolbox development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import concurrent.futures
import json
import sys
import time
import traceback

from tornado.ioloop import IOLoop
from tornado.web import Application
from tornado.websocket import WebSocketHandler

from .jsonrpcmonitor import JsonRpcWebSocketMonitor
from ..monitor import Cancellation
from ..opmetainf import OpMetaInfo

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

_DEBUG_WEB_SOCKET_RPC = False

CANCEL_METHOD_NAME = '__cancel__'

ERROR_CODE_METHOD_MISSING_OR_INVALID = 10
ERROR_CODE_METHOD_RAISED_EXCEPTION = 20
ERROR_CODE_METHOD_RESPONSE_NOT_SERIALIZABLE = 30
ERROR_CODE_METHOD_NOT_SUPPORTED = 40
ERROR_CODE_CANCEL_IS_INVALID = 50
ERROR_CODE_METHOD_EXECUTION_CANCELLED = 999


# noinspection PyAbstractClass
class JsonRpcWebSocketHandler(WebSocketHandler):
    """
    A Tornado WebSockets handler that represents a JSON-RPC 2.0 endpoint.

    :param application: Tornado application object
    :param request: Tornado request
    :param service_factory: A function that returns the object providing the this service's callable methods.
    :param report_defer_period: The time in seconds between two subsequent progress reports reported to
           a monitor passed to a service method
    :param kwargs: Keyword-arguments passed to the request handler.
    """

    def __init__(self,
                 application: Application,
                 request,
                 service_factory=None,
                 report_defer_period: float = None,
                 **kwargs):
        super(JsonRpcWebSocketHandler, self).__init__(application, request, **kwargs)
        if not service_factory:
            raise ValueError('service_factory must be provided')
        self._application = application
        self._service_factory = service_factory
        self._service = None
        self._service_method_meta_infos = None
        self._thread_pool = concurrent.futures.ThreadPoolExecutor()
        self._active_monitors = {}
        self._active_futures = {}
        self._job_start = {}
        self._report_defer_period = report_defer_period

    def open(self):
        if _DEBUG_WEB_SOCKET_RPC:
            print("DEBUG: JsonRpcWebSocketHandler.open")
        self._service = self._service_factory(self._application)
        self._service_method_meta_infos = {}

        # noinspection PyBroadException
        try:
            # Reduce 200-500ms delays due to the interaction between Nagle’s algorithm and TCP delayed ACKs
            # at the expense of possibly increasing bandwidth usage.
            self.set_nodelay(True)
        except:
            pass

    def on_close(self):
        if _DEBUG_WEB_SOCKET_RPC:
            print("DEBUG: JsonRpcWebSocketHandler.on_close")
        self._service = None
        self._service_method_meta_infos = None

    # We must override this to return True (= all origins are ok), otherwise we get
    #   WebSocket connection to 'ws://localhost:9090/app' failed:
    #   Error during WebSocket handshake:
    #   Unexpected response code: 403 (forbidden)
    def check_origin(self, origin):
        if _DEBUG_WEB_SOCKET_RPC:
            print("DEBUG: JsonRpcWebSocketHandler.check_origin(%s)" % repr(origin))
        return True

    def on_message(self, message: str):
        # Note, the following error cases 1-4 cannot be communicated to client as we
        # haven't got a valid method "id" which is required for a JSON-RPC response

        # noinspection PyBroadException
        try:
            message_obj = json.loads(message)
        except:
            print("ERROR: Failed to parse incoming JSON-RPC message: {}".format(message))
            traceback.print_exc(file=sys.stdout)
            return 1  # for testing only

        if not isinstance(message_obj, type({})):
            print('ERROR: Received invalid JSON-RPC message '
                  'of unexpected type "{}": {}'.format(type(message_obj), message))
            return 2  # for testing only

        method_id = message_obj.get('id', None)
        if not isinstance(method_id, int):
            print('ERROR: Received invalid JSON-RPC message: '
                  'missing or invalid "id" value: {}'.format(message))
            return 3  # for testing only

        method_name = message_obj.get('method', None)
        # noinspection PyTypeChecker
        if not isinstance(method_name, str) or len(method_name) == 0:
            print('ERROR: Received invalid JSON-RPC message: '
                  'missing or invalid "method" value: {}'.format(message))
            self._write_json_rpc_error_response(method_id, ERROR_CODE_METHOD_MISSING_OR_INVALID,
                                                'Missing or invalid method')
            return 4  # for testing only

        if _DEBUG_WEB_SOCKET_RPC:
            print("DEBUG: RPC [%s] ==> %s: %s" % (method_id, method_name, message))

        method_params = message_obj.get('params', None)

        if hasattr(self._service, method_name):
            self._job_start[method_id] = time.time()
            future = self._thread_pool.submit(self.call_service_method,
                                              method_id, method_name, method_params)
            self._active_futures[method_id] = future

            def _send_service_method_result(f: concurrent.futures.Future) -> None:
                assert method_id is not None
                # noinspection PyTypeChecker
                self.send_service_method_result(method_id, method_name, f)

            IOLoop.current().add_future(future=future, callback=_send_service_method_result)
        elif method_name == CANCEL_METHOD_NAME:
            job_id = method_params.get('id') if method_params else None
            if not isinstance(job_id, int):
                print('ERROR: Received invalid JSON-RPC message: '
                      'missing or invalid "id" parameter for method "{}": {}'
                      .format(CANCEL_METHOD_NAME, message))
                self._write_json_rpc_error_response(method_id, ERROR_CODE_CANCEL_IS_INVALID,
                                                    'Invalid cancellation request')
                return 5  # for testing only
            # cancel progress monitor
            if job_id in self._active_monitors:
                self._active_monitors[job_id].cancel()
                del self._active_monitors[job_id]
            # cancel future
            if job_id in self._active_futures:
                self._active_futures[job_id].cancel()
                del self._active_futures[job_id]
            if job_id in self._job_start:
                delta_t = time.time() - self._job_start[job_id]
                del self._job_start[job_id]
                if _DEBUG_WEB_SOCKET_RPC:
                    print('DEBUG: Cancelled {}() call (id={}) after {} seconds'.format(method_name, job_id, delta_t))
            self._write_json_rpc_result_response(method_id, method_name)
        else:
            print('ERROR: Received invalid JSON-RPC message: '
                  'unsupported method: {}'.format(message))
            self._write_json_rpc_error_response(method_id, ERROR_CODE_METHOD_NOT_SUPPORTED,
                                                'Unsupported method: {}'.format(method_name))
            return 6  # for testing only

    def send_service_method_result(self, method_id: int, method_name: str, future: concurrent.futures.Future):
        try:
            result = future.result()
        except (concurrent.futures.CancelledError, Cancellation):
            return self._write_json_rpc_error_response(method_id, ERROR_CODE_METHOD_EXECUTION_CANCELLED,
                                                       '{}() call cancelled'.format(method_name))
        except Exception as e:
            stack_trace = traceback.format_exc()
            print(stack_trace, file=sys.stderr, flush=True)
            return self._write_json_rpc_error_response(method_id, ERROR_CODE_METHOD_RAISED_EXCEPTION,
                                                       '{}() call raised exception: "{}"'.format(method_name, e),
                                                       data=stack_trace)

        if method_id in self._active_monitors:
            del self._active_monitors[method_id]
        if method_id in self._active_futures:
            del self._active_futures[method_id]
        if method_id in self._job_start:
            delta_t = time.time() - self._job_start[method_id]
            del self._job_start[method_id]
            if _DEBUG_WEB_SOCKET_RPC:
                print('DEBUG: Finished {}() call (id={}) after {} seconds'.format(method_name, method_id, delta_t))

        self._write_json_rpc_result_response(method_id, method_name, result=result)

    def _write_json_rpc_result_response(self, method_id: int, method_name: str, result=None) -> bool:
        success = self._write_json_rpc_response(dict(jsonrpc='2.0',
                                                     id=method_id,
                                                     response=result))
        if not success:
            self._write_json_rpc_error_response(method_id, ERROR_CODE_METHOD_RESPONSE_NOT_SERIALIZABLE,
                                                '{}() call returned a value that could not be converted to JSON'
                                                .format(method_name))
        return success

    def _write_json_rpc_error_response(self, method_id: int, code: int, message: str, data=None) -> bool:
        return self._write_json_rpc_response(dict(jsonrpc='2.0',
                                                  id=method_id,
                                                  error=dict(code=code,
                                                             message=message,
                                                             data=data)))

    def _write_json_rpc_response(self, json_rpc_response: dict) -> bool:
        # noinspection PyBroadException
        try:
            json_text = json.dumps(json_rpc_response)
        except Exception:
            stack_trace = traceback.format_exc()
            print(stack_trace, file=sys.stderr, flush=True)
            return False

        self.write_message(json_text)

        if _DEBUG_WEB_SOCKET_RPC:
            method_id = json_rpc_response.get('id')
            print("DEBUG: RPC [%s] <== %s" % (method_id, json_text))

        return True

    def call_service_method(self, method_id: int, method_name: str, method_params: list):

        if _DEBUG_WEB_SOCKET_RPC:
            print('DEBUG: Calling {}() (id={})'.format(method_name, method_id))

        assert self._service is not None
        method = getattr(self._service, method_name)

        assert self._service_method_meta_infos is not None
        op_meta_info = self._service_method_meta_infos.get(method_name)
        if op_meta_info is None:
            op_meta_info = OpMetaInfo.introspect_operation(method)
            self._service_method_meta_infos[method_name] = op_meta_info

        # Check if we need a ProgressMonitor impl. here.
        if op_meta_info.has_monitor:
            # The impl. will send "progress" messages via the web-socket.
            monitor = JsonRpcWebSocketMonitor(method_id, self, report_defer_period=self._report_defer_period)
            self._active_monitors[method_id] = monitor
            if isinstance(method_params, type([])):
                result = method(*method_params, monitor=monitor)
            elif isinstance(method_params, type({})):
                result = method(**method_params, monitor=monitor)
            else:
                result = method(monitor=monitor)
        else:
            if isinstance(method_params, type([])):
                result = method(*method_params)
            elif isinstance(method_params, type({})):
                result = method(**method_params)
            else:
                result = method()

        return result


def set_debug_web_socket_rpc(value: bool):
    """ For testing only """
    global _DEBUG_WEB_SOCKET_RPC
    _DEBUG_WEB_SOCKET_RPC = value
