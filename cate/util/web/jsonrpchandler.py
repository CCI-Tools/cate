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
import logging
import sys
import time
import traceback
from typing import Any, Optional, Tuple

from tornado.ioloop import IOLoop
from tornado.web import Application
from tornado.websocket import WebSocketHandler

from .jsonrpcmonitor import JsonRpcWebSocketMonitor
from .common import exception_to_json, log_debug
from ..monitor import Cancellation
from ..opmetainf import OpMetaInfo

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

_LOG = logging.getLogger('cate')

CANCEL_METHOD_NAME = '__cancel__'

# See http://www.jsonrpc.org/specification#error_object
# The error codes from and including -32768 to -32000 are reserved for pre-defined errors.
ERROR_CODE_INVALID_REQUEST = -32600
ERROR_CODE_METHOD_NOT_FOUND = -32601
ERROR_CODE_INVALID_PARAMS = -32602
# The error codes from and including -32000 to -32099 are reserved for implementation-defined server-errors.
ERROR_CODE_OS_ERROR = -32001
ERROR_CODE_OUT_OF_MEMORY = -32002
ERROR_CODE_METHOD_ERROR = -32003
ERROR_CODE_INVALID_RESPONSE = -32004
# The remainder of the space is available for application defined errors.
ERROR_CODE_CANCELLED = 999


# noinspection PyAbstractClass
class JsonRpcWebSocketHandler(WebSocketHandler):
    """
    A Tornado WebSockets handler that represents a JSON-RPC 2.0 endpoint.

    :param application: Tornado application object
    :param request: Tornado request
    :param service_factory: A function that returns the object providing the this service's callable methods.
    :param validation_exception_class: The exception type used to signal parameter validation errors.
           Must derive from ``BaseException``.
    :param report_defer_period: The time in seconds between two subsequent progress reports reported to
           a monitor passed to a service method
    :param kwargs: Keyword-arguments passed to the request handler.
    """

    def __init__(self,
                 application: Application,
                 request,
                 service_factory=None,
                 validation_exception_class: type = None,
                 report_defer_period: float = None,
                 **kwargs):
        super(JsonRpcWebSocketHandler, self).__init__(application, request, **kwargs)
        if service_factory is None:
            raise ValueError('service_factory must be given')
        if validation_exception_class is None:
            raise ValueError('validation_exception_class must be given')
        self._application = application
        self._service_factory = service_factory
        self._validation_exception_class = validation_exception_class
        self._report_defer_period = report_defer_period
        self._service = None
        self._service_method_meta_infos = None
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(thread_name_prefix='JsonRpcWebSocketHandler')
        self._active_monitors = {}
        self._active_futures = {}

    def open(self):
        log_debug("open")
        self._service = self._service_factory(self._application)
        self._service_method_meta_infos = {}

        # noinspection PyBroadException
        try:
            # Reduce 200-500ms delays due to the interaction between Nagle’s algorithm and TCP delayed ACKs
            # at the expense of possibly increasing bandwidth usage.
            self.set_nodelay(True)
        except Exception:
            pass

    def on_close(self):
        log_debug("on_close")
        self._thread_pool.shutdown(wait=False)
        self._service = None
        self._service_method_meta_infos = None

    # We must override this to return True (= all origins are ok), otherwise we get
    #   WebSocket connection to 'ws://localhost:9090/app' failed:
    #   Error during WebSocket handshake:
    #   Unexpected response code: 403 (forbidden)
    def check_origin(self, origin):
        log_debug('check_origin:', repr(origin))
        return True

    def on_message(self, message: str):
        _LOG.debug('JSON RPC message: %s' % message)

        # Note, the following error cases 1-4 cannot be communicated to client as we
        # haven't got a valid method "id" which is required for a JSON-RPC response

        # noinspection PyBroadException
        try:
            message_obj = json.loads(message)
        except Exception:
            _LOG.exception('Failed to parse incoming JSON-RPC message: %s' % message)
            return 1  # for testing only

        if not isinstance(message_obj, type({})):
            _LOG.error('Received JSON-RPC message with unexpected type: %s' % message)
            return 2  # for testing only

        method_id = message_obj.get('id', None)
        if not isinstance(method_id, int):
            _LOG.error('Received invalid JSON-RPC message: missing or invalid "id" value: %s' % message)
            return 3  # for testing only

        method_name = message_obj.get('method', None)
        # noinspection PyTypeChecker
        if not isinstance(method_name, str) or len(method_name) == 0:
            _LOG.error('Received invalid JSON-RPC message: missing or invalid "method" value: %s' % message)
            self._write_json_rpc_error_response(method_id,
                                                ERROR_CODE_METHOD_NOT_FOUND,
                                                'Method not found or invalid.',
                                                method_name=method_name)
            return 4  # for testing only

        method_params = message_obj.get('params', None)

        if hasattr(self._service, method_name):
            log_debug('Submit:', method_id, method_name, method_params)
            future = self._thread_pool.submit(self.call_service_method,
                                              method_id, method_name, method_params)
            self._active_futures[method_id] = future

            def _send_service_method_result(f: concurrent.futures.Future) -> None:
                assert method_id is not None
                log_debug('Returned: ', method_id, method_name, method_params)
                self.send_service_method_result(method_id, method_name, f)

            # future.add_done_callback(_send_service_method_result)
            IOLoop.current().add_future(future, _send_service_method_result)

        elif method_name == CANCEL_METHOD_NAME:
            job_id = method_params.get('id') if method_params else None
            if not isinstance(job_id, int):
                _LOG.error('Received invalid JSON-RPC message: '
                              'missing or invalid "id" parameter for method "{}": {}'
                              .format(CANCEL_METHOD_NAME, message))
                self._write_json_rpc_error_response(method_id,
                                                    ERROR_CODE_INVALID_REQUEST,
                                                    'Invalid cancellation request.')
                return 5  # for testing only
            # cancel progress monitor
            if job_id in self._active_monitors:
                self._active_monitors[job_id].cancel()
                del self._active_monitors[job_id]
            # cancel future
            if job_id in self._active_futures:
                self._active_futures[job_id].cancel()
                del self._active_futures[job_id]
            self._write_json_rpc_result_response(method_id, method_name)

        else:
            _LOG.error('Received invalid JSON-RPC message: unsupported method: %s' % message)
            self._write_json_rpc_error_response(method_id,
                                                ERROR_CODE_INVALID_REQUEST,
                                                "Invalid request.",
                                                method_name=method_name)
            return 6  # for testing only

    def send_service_method_result(self, method_id: int, method_name: str, future: concurrent.futures.Future) -> bool:

        result = None
        message = ''
        code = None
        exc_info = None

        # see https://docs.python.org/3/library/exceptions.html#exception-hierarchy
        # noinspection PyBroadException
        try:
            result = future.result()
        except self._validation_exception_class:
            exc_info = sys.exc_info()
            code = ERROR_CODE_INVALID_PARAMS
        except (concurrent.futures.CancelledError, Cancellation):
            exc_info = sys.exc_info()
            code = ERROR_CODE_CANCELLED
            message = 'Cancelled.'
        except MemoryError:
            exc_info = sys.exc_info()
            code = ERROR_CODE_OUT_OF_MEMORY
            message = 'Out of memory.'
        except OSError:
            exc_info = sys.exc_info()
            code = ERROR_CODE_OS_ERROR
        except Exception:
            exc_info = sys.exc_info()
            code = ERROR_CODE_METHOD_ERROR

        if exc_info:
            return self._write_json_rpc_error_response(method_id,
                                                       code,
                                                       message=message or str(exc_info[1]),
                                                       method_name=method_name,
                                                       exc_info=exc_info)

        if method_id in self._active_monitors:
            del self._active_monitors[method_id]
        if method_id in self._active_futures:
            del self._active_futures[method_id]

        return self._write_json_rpc_result_response(method_id, method_name, result=result)

    def _write_json_rpc_result_response(self, method_id: int, method_name: str, result=None) -> bool:
        exc_info = self._write_json_rpc_response(dict(jsonrpc='2.0',
                                                      id=method_id,
                                                      response=result))
        if exc_info:
            self._write_json_rpc_error_response(method_id,
                                                ERROR_CODE_INVALID_RESPONSE,
                                                'Invalid response (not JSON-serializable).'.format(method_name),
                                                method_name=method_name,
                                                exc_info=exc_info)
            return False
        return True

    def _write_json_rpc_error_response(self,
                                       method_id: int,
                                       code: int,
                                       message: str,
                                       method_name: str = None,
                                       exc_info=None) -> bool:
        if exc_info:
            if code != ERROR_CODE_CANCELLED:
                exc_type, exc_value, exc_tb = exc_info
                _LOG.error(''.join(traceback.format_exception(exc_type, exc_value, exc_tb)))
            data = exception_to_json(exc_info, method=method_name)
        else:
            data = dict(method=method_name)
        exc_info = self._write_json_rpc_response(dict(jsonrpc='2.0',
                                                      id=method_id,
                                                      error=dict(code=code,
                                                                 message=message,
                                                                 data=data)))
        return exc_info is None

    def _write_json_rpc_response(self, json_rpc_response: dict) -> Optional[Tuple[type, Any, Any]]:
        # noinspection PyBroadException
        try:
            json_text = json.dumps(json_rpc_response)
            log_debug('Writing:', json_text)
            IOLoop.current().add_callback(self.write_message, json_text)
        except Exception:
            return sys.exc_info()

        return None

    def call_service_method(self, method_id: int, method_name: str, method_params: list):

        log_debug('Started:', method_id, method_name, method_params)
        t0 = time.time()

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

        log_debug('Ended:', method_id, method_name, result, time.time() - t0)

        return result
