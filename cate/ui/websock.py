# The MIT License (MIT)
# Copyright (c) 2016 by the Cate Development Team and contributors
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
from typing import Tuple

import xarray as xr
import tornado.websocket
from tornado.ioloop import IOLoop

from cate.core.ds import DATA_STORE_REGISTRY
from cate.core.monitor import Monitor
from cate.core.op import OpMetaInfo, OP_REGISTRY
from cate.core.util import cwd
from cate.ui.conf import WEBAPI_PROGRESS_DEFER_PERIOD
from cate.ui.wsmanag import WorkspaceManager


def type_to_str(data_type):
    if isinstance(data_type, str):
        return data_type
    elif hasattr(data_type, '__name__'):
        return data_type.__name__
    else:
        return str(data_type)


class ServiceMethods:
    """
    Object which implements Cate's server-side methods.

    All methods receive inputs deserialized from JSON-RCP requests and must
    return JSON-serializable outputs.

    :param: workspace_manager The current workspace manager.
    """

    def __init__(self, workspace_manager: WorkspaceManager):
        self.workspace_manager = workspace_manager

    # noinspection PyMethodMayBeStatic
    def get_data_stores(self) -> list:
        """
        Get registered data stores.

        :return: JSON-serializable list of data stores, sorted by name.
        """
        data_stores = DATA_STORE_REGISTRY.get_data_stores()
        data_store_list = []
        for data_store in data_stores:
            data_store_list.append(dict(id=data_store.name,
                                        name=data_store.name,
                                        description=''))

        return sorted(data_store_list, key=lambda ds: ds['name'])

    # noinspection PyMethodMayBeStatic
    def get_data_sources(self, data_store_id: str, monitor: Monitor) -> list:
        """
        Get data sources for a given data store.

        :param data_store_id: ID of the data store
        :param monitor: a progress monitor
        :return: JSON-serializable list of data sources, sorted by name.
        """
        data_store = DATA_STORE_REGISTRY.get_data_store(data_store_id)
        if data_store is None:
            raise ValueError('Unknown data store: "%s"' % data_store_id)

        data_sources = data_store.query(monitor=monitor)
        data_source_list = []
        for data_source in data_sources:
            data_source_list.append(dict(id=data_source.name,
                                         name=data_source.name,
                                         meta_info=data_source.meta_info))

        return sorted(data_source_list, key=lambda ds: ds['name'])

    # noinspection PyMethodMayBeStatic
    def get_operations(self) -> list:
        """
        Get registered operations.

        :return: JSON-serializable list of data sources, sorted by name.
        """
        op_list = []
        for op_name, op_reg in OP_REGISTRY.op_registrations.items():
            op_meta_info = op_reg.op_meta_info
            inputs = []
            for input_name, input_props in op_meta_info.input.items():
                inputs.append(dict(name=input_name,
                                   dataType=type_to_str(input_props.get('data_type', 'str')),
                                   description=input_props.get('description', '')))
            outputs = []
            for output_name, output_props in op_meta_info.output.items():
                outputs.append(dict(name=output_name,
                                    dataType=type_to_str(output_props.get('data_type', 'str')),
                                    description=output_props.get('description', '')))
            op_list.append(dict(name=op_name,
                                tags=op_meta_info.header.get('tags', []),
                                description=op_meta_info.header.get('description', ''),
                                inputs=inputs,
                                outputs=outputs))

        return sorted(op_list, key=lambda op: op['name'])

    # see cate-desktop: src/renderer.states.WorkspaceState
    def new_workspace(self) -> dict:
        workspace = self.workspace_manager.new_workspace(None)
        return workspace.to_json_dict()

    # noinspection PyAbstractClass
    def set_workspace_resource(self, base_dir: str, res_name: str, op_name: str, op_args: dict,
                               monitor: Monitor) -> dict:
        # TODO (nf): op_args come in as ["name1=value1", "name2=value2", ...] due to the CLI and REST API
        #            If this called from cate-desktop, op_args could already be a proper typed + validated JSON dict
        op_args = ['%s=%s' % (k, ('"%s"' % v) if isinstance(v, str) else v) for k, v in op_args.items()]
        with cwd(base_dir):
            workspace = self.workspace_manager.set_workspace_resource(base_dir,
                                                                      res_name,
                                                                      op_name,
                                                                      op_args=op_args,
                                                                      monitor=monitor)
            return workspace.to_json_dict()

    def get_color_maps(self):
        from .cmaps import get_cmaps
        return get_cmaps()

    def get_workspace_variable_statistics(self, base_dir: str, res_name: str, var_name: str, var_index: Tuple[int]):
        workspace_manager = self.workspace_manager
        workspace = workspace_manager.get_workspace(base_dir, do_open=False)
        if res_name not in workspace.resource_cache:
            raise ValueError('Unknown resource "%s"' % res_name)

        dataset = workspace.resource_cache[res_name]
        if not isinstance(dataset, xr.Dataset):
            raise ValueError('Resource "%s" must be a Dataset' % res_name)

        if var_name not in dataset:
            raise ValueError('Variable "%s" not found in "%s"' % (var_name, res_name))

        variable = dataset[var_name]
        if var_index:
            variable = variable[var_index]

        valid_min = variable.min(skipna=True)
        valid_max = variable.max(skipna=True)

        return dict(min=float(valid_min), max=float(valid_max))

# noinspection PyAbstractClass
class AppWebSocketHandler(tornado.websocket.WebSocketHandler):
    def __init__(self, application, request, **kwargs):
        super(AppWebSocketHandler, self).__init__(application, request, **kwargs)
        if not application.workspace_manager:
            raise Exception('missing workspace manager')
        self._workspace_manager = application.workspace_manager
        self._thread_pool = concurrent.futures.ThreadPoolExecutor()
        self._service = None
        self._active_monitors = {}
        self._active_futures = {}
        # Check: following call causes exception although Tornado docs say, it is ok
        # self.set_nodelay(True)

    def open(self):
        print("AppWebSocketHandler.open")
        self._service = ServiceMethods(self._workspace_manager)

    def on_close(self):
        print("AppWebSocketHandler.on_close")
        self._service = None

    # We must override this to return True (= all origins are ok), otherwise we get
    #   WebSocket connection to 'ws://localhost:9090/app' failed:
    #   Error during WebSocket handshake:
    #   Unexpected response code: 403 (forbidden)
    def check_origin(self, origin):
        print("AppWebSocketHandler: check " + str(origin))
        return True

    # TODO: notify connected client on any of the following error cases

    def on_message(self, message: str):
        print("AppWebSocketHandler.on_message('%s')" % message)

        # noinspection PyBroadException
        try:
            message_obj = json.loads(message)
        except:
            print("Failed to parse incoming JSON-RCP message: %s" % message)
            traceback.print_exc(file=sys.stdout)
            return

        if not isinstance(message_obj, type({})):
            print('Received invalid JSON-RCP message of unexpected type "%s": %s' % (type(message_obj), message))
            return

        method_id = message_obj.get('id', None)
        if not isinstance(method_id, int):
            print('Received invalid JSON-RCP message: missing or invalid "id" value: %s' % message)
            return

        method_name = message_obj.get('method', None)
        # noinspection PyTypeChecker
        if not isinstance(method_name, str) or len(method_name) == 0:
            print('Received invalid JSON-RCP message: missing or invalid "method" value: %s' % message)
            return

        method_params = message_obj.get('params', None)

        if hasattr(self._service, method_name):
            future = self._thread_pool.submit(self.call_service_method, method_id, method_name, method_params)
            self._active_futures[method_id] = future

            def _send_service_method_result(f: concurrent.futures.Future) -> None:
                assert method_id is not None
                # noinspection PyTypeChecker
                self.send_service_method_result(method_id, f)

            IOLoop.current().add_future(future=future, callback=_send_service_method_result)
        elif method_name == '__cancelJob__':
            job_id = method_params.get('jobId', None)
            if not isinstance(job_id, int):
                print('Received invalid JSON-RCP message: missing or invalid "jobId" value: %s' % message)
                return
            # TODO: check if the following code requires thread sync
            # cancel progress monitor
            if job_id in self._active_monitors:
                self._active_monitors[job_id].cancel()
                del self._active_monitors[job_id]
            # cancel future
            if job_id in self._active_futures:
                self._active_futures[job_id].cancel()
                del self._active_futures[job_id]
            response = dict(jsonrcp='2.0', id=method_id)
            self._write_response(json.dumps(response))
        else:
            response = dict(jsonrcp='2.0',
                            id=method_id,
                            error=dict(code=20,
                                       message='Unsupported method: %s' % method_name))
            self._write_response(json.dumps(response))



    def send_service_method_result(self, method_id: int, future: concurrent.futures.Future):
        try:
            result = future.result()
            response = dict(jsonrcp='2.0',
                            id=method_id,
                            response=result)
            if method_id in self._active_monitors:
                del self._active_monitors[method_id]
            if method_id in self._active_futures:
                del self._active_futures[method_id]
        except (concurrent.futures.CancelledError, InterruptedError):
            response = dict(jsonrcp='2.0',
                            id=method_id,
                            error=dict(code=999,
                                       message='Cancelled'))
        except Exception as e:
            stack_trace = traceback.format_exc()
            response = dict(jsonrcp='2.0',
                            id=method_id,
                            error=dict(code=10,
                                       message='Exception caught: %s' % e,
                                       data=stack_trace))
        try:
            json_text = json.dumps(response)
        except Exception as e:
            stacktrace = traceback.format_exc()
            print('INTERNAL ERROR: response could not be converted to JSON: %s' % e)
            print('response = %s' % response)
            print(stacktrace)
            json_text = json.dumps(dict(jsonrcp='2.0',
                                        id=method_id,
                                        error=dict(code=30,
                                                   message='Exception caught: %s' % e,
                                                   data=stacktrace)))
        self._write_response(json_text)

    def _write_response(self, json_text):
        print('<== ', json_text)
        self.write_message(json_text)

    def call_service_method(self, method_id, method_name, method_params):
        method = getattr(self._service, method_name)

        # Check if we need a ProgressMonitor impl. here.
        op_meta_info = OpMetaInfo.introspect_operation(method)
        if op_meta_info.has_monitor:
            # The impl. will send "progress" messages via the web-socket.
            monitor = WebSocketMonitor(method_id, self)
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


class WebSocketMonitor(Monitor):
    def __init__(self, method_id: int, handler: tornado.websocket.WebSocketHandler):
        self.method_id = method_id
        self.handler = handler
        self._cancelled = False
        self.last_time = None

        self.label = None
        self.total = None
        self.worked = None

    def _write_progress(self, message=None):
        current_time = time.time()
        if not self.last_time or (current_time - self.last_time) >= WEBAPI_PROGRESS_DEFER_PERIOD:

            progress = {}
            if self.label is not None:
                progress['label'] = self.label
            if message is not None:
                progress['message'] = message
            if self.total is not None:
                progress['total'] = self.total
            if self.worked is not None:
                progress['worked'] = self.worked

            self.handler.write_message(json.dumps(dict(jsonrcp="2.0",
                                                       id=self.method_id,
                                                       progress=progress)))
            self.last_time = current_time

    def start(self, label: str, total_work: float = None):
        self.label = label
        self.total = total_work
        self.worked = 0.0 if total_work else None
        self._write_progress(message='Started')

    def progress(self, work: float = None, msg: str = None):
        if work:
            self.worked = (self.worked or 0.0) + work
        self._write_progress(message=msg)

    def done(self):
        self.worked = self.total
        self._write_progress(message='Done')

    def cancel(self):
        self._cancelled = True

    def is_cancelled(self) -> bool:
        return self._cancelled
