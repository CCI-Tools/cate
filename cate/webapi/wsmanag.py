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

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

import json
import urllib.parse
import urllib.request
from tornado import gen, ioloop, websocket
from typing import List

from cate.conf.defaults import WEBAPI_WORKSPACE_TIMEOUT, WEBAPI_RESOURCE_TIMEOUT, WEBAPI_PLOT_TIMEOUT
from cate.core.workspace import Workspace, WorkspaceError, OpKwArgs
from cate.core.wsmanag import WorkspaceManager
from cate.util import encode_url_path, Monitor


class WebAPIWorkspaceManager(WorkspaceManager):
    """
    Implementation of the WorkspaceManager interface against a REST API.
    """

    def __init__(self, service_info: dict, timeout=120):
        address = service_info.get('address', None) or '127.0.0.1'
        port = service_info.get('port', None)
        if not port:
            raise ValueError('missing "port" number in service_info argument')
        self.base_url = 'http://%s:%s' % (address, port)
        self.ws_url = 'ws://%s:%s/api' % (address, port)
        self.ws_client = WebSocketClient(self.ws_url, timeout)
        self.ws_client.connect()
        self.timeout = timeout

    def _url(self, path_pattern: str, path_args: dict = None, query_args: dict = None) -> str:
        return self.base_url + encode_url_path(path_pattern, path_args=path_args, query_args=query_args)

    def _ws_json_rpc(self, method, params, error_type=WorkspaceError, timeout: float = None,
                     monitor: Monitor = Monitor.NONE):
        json_rpc_response = self.ws_client.invokeMethod(method, params, timeout=timeout, monitor=monitor)
        json_response = json.loads(json_rpc_response)
        if 'error' in json_response:
            error_details = json_response.get('error')
            message = error_details.get('message') if error_details else None
            trace_back = error_details.get('data') if error_details else None
            message = message or ''
            if trace_back:
                message += self.get_traceback_header() + trace_back
            raise error_type(message)
        return json_response.get('response')

    def _fetch_json(self, url, data=None, error_type=WorkspaceError, timeout: float = None):
        with urllib.request.urlopen(url, data=data, timeout=timeout or self.timeout) as response:
            json_text = response.read()
        json_response = json.loads(json_text.decode('utf-8'))
        status = json_response.get('status')
        if status == 'error':
            error_details = json_response.get('error')
            message = error_details.get('message') if error_details else None
            type_name = error_details.get('type') if error_details else None
            trace_back = error_details.get('traceback') if error_details else None
            message = message or type_name or ''
            if trace_back:
                message += self.get_traceback_header() + trace_back
            raise error_type(message)
        return json_response.get('content')

    # noinspection PyMethodMayBeStatic
    def _query(self, **kwargs: dict):
        return {key: value for key, value in kwargs.items() if value is not None}

    def _post_data(self, **kwargs: dict):
        data = urllib.parse.urlencode(self._query(**kwargs))
        return data.encode() if data else None

    @classmethod
    def get_traceback_header(cls) -> str:
        traceback_title = 'Cate WebAPI service traceback'
        traceback_line = len(traceback_title) * '='
        return '\n' + traceback_line + '\n' + traceback_title + '\n' + traceback_line + '\n'

    def is_running(self, timeout: float = None) -> bool:
        # noinspection PyBroadException
        try:
            self._fetch_json('/', timeout=timeout)
            return True
        except WorkspaceError:
            return True
        except:
            return False

    def get_open_workspaces(self) -> List[Workspace]:
        json_list = self._ws_json_rpc("get_open_workspaces", dict(), timeout=WEBAPI_WORKSPACE_TIMEOUT)
        return [Workspace.from_json_dict(ws_json_dict) for ws_json_dict in json_list]

    def get_workspace(self, base_dir: str) -> Workspace:
        json_dict = self._ws_json_rpc("get_workspace", dict(base_dir=base_dir), timeout=WEBAPI_WORKSPACE_TIMEOUT)
        return Workspace.from_json_dict(json_dict)

    def new_workspace(self, base_dir: str, description: str = None) -> Workspace:
        json_dict = self._ws_json_rpc("new_workspace", dict(base_dir=base_dir, description=description),
                                      timeout=WEBAPI_WORKSPACE_TIMEOUT)
        return Workspace.from_json_dict(json_dict)

    def open_workspace(self, base_dir: str, monitor: Monitor = Monitor.NONE) -> Workspace:
        json_dict = self._ws_json_rpc("open_workspace", dict(base_dir=base_dir),
                                      timeout=WEBAPI_WORKSPACE_TIMEOUT,
                                      monitor=monitor)
        return Workspace.from_json_dict(json_dict)

    def close_workspace(self, base_dir: str) -> None:
        self._ws_json_rpc("close_workspace", dict(base_dir=base_dir), timeout=WEBAPI_WORKSPACE_TIMEOUT)

    def close_all_workspaces(self) -> None:
        self._ws_json_rpc("close_all_workspaces", dict(), timeout=WEBAPI_WORKSPACE_TIMEOUT)

    def save_workspace_as(self, base_dir: str, to_dir: str, monitor: Monitor = Monitor.NONE) -> Workspace:
        json_dict = self._ws_json_rpc("save_workspace_as",
                                      dict(base_dir=base_dir, to_dir=to_dir),
                                      timeout=WEBAPI_WORKSPACE_TIMEOUT,
                                      monitor=monitor)
        return Workspace.from_json_dict(json_dict)

    def save_workspace(self, base_dir: str, monitor: Monitor = Monitor.NONE) -> Workspace:
        json_dict = self._ws_json_rpc("save_workspace", dict(base_dir=base_dir),
                                      timeout=WEBAPI_WORKSPACE_TIMEOUT,
                                      monitor=monitor)
        return Workspace.from_json_dict(json_dict)

    def save_all_workspaces(self, monitor: Monitor = Monitor.NONE) -> None:
        self._ws_json_rpc("save_all_workspaces", dict(), timeout=WEBAPI_WORKSPACE_TIMEOUT, monitor=monitor)

    def delete_workspace(self, base_dir: str) -> None:
        self._ws_json_rpc("delete_workspace", dict(base_dir=base_dir), timeout=WEBAPI_WORKSPACE_TIMEOUT)

    def clean_workspace(self, base_dir: str) -> Workspace:
        json_dict = self._ws_json_rpc("clean_workspace", dict(base_dir=base_dir), timeout=WEBAPI_WORKSPACE_TIMEOUT)
        return Workspace.from_json_dict(json_dict)

    def run_op_in_workspace(self, base_dir: str, op_name: str, op_args: OpKwArgs,
                            monitor: Monitor = Monitor.NONE) -> Workspace:
        json_dict = self._ws_json_rpc("run_op_in_workspace",
                                      dict(base_dir=base_dir, op_name=op_name, op_args=op_args),
                                      timeout=WEBAPI_WORKSPACE_TIMEOUT,
                                      monitor=monitor)
        return Workspace.from_json_dict(json_dict)

    def delete_workspace_resource(self, base_dir: str, res_name: str) -> Workspace:
        json_dict = self._ws_json_rpc("delete_workspace_resource",
                                      dict(base_dir=base_dir, res_name=res_name),
                                      timeout=WEBAPI_RESOURCE_TIMEOUT)
        return Workspace.from_json_dict(json_dict)

    def set_workspace_resource_persistence(self, base_dir: str, res_name: str, persistent: bool) -> Workspace:
        json_dict = self._ws_json_rpc("set_workspace_resource_persistence",
                                      dict(base_dir=base_dir, res_name=res_name, persistent=persistent),
                                      timeout=WEBAPI_RESOURCE_TIMEOUT)
        return Workspace.from_json_dict(json_dict)

    def set_workspace_resource(self, base_dir: str, res_name: str,
                               op_name: str, op_args: OpKwArgs,
                               monitor: Monitor = Monitor.NONE) -> Workspace:
        json_dict = self._ws_json_rpc("set_workspace_resource",
                                      dict(base_dir=base_dir, res_name=res_name, op_name=op_name,
                                           op_args=op_args),
                                      timeout=WEBAPI_RESOURCE_TIMEOUT,
                                      monitor=monitor)
        return Workspace.from_json_dict(json_dict)

    def rename_workspace_resource(self, base_dir: str,
                                  res_name: str, new_res_name: str) -> Workspace:
        json_dict = self._ws_json_rpc("rename_workspace_resource",
                                      dict(base_dir=base_dir, res_name=res_name, new_res_name=new_res_name),
                                      timeout=WEBAPI_RESOURCE_TIMEOUT)
        return Workspace.from_json_dict(json_dict)

    def write_workspace_resource(self, base_dir: str, res_name: str,
                                 file_path: str, format_name: str = None,
                                 monitor: Monitor = Monitor.NONE) -> None:
        self._ws_json_rpc("write_workspace_resource",
                          dict(base_dir=base_dir, res_name=res_name,
                               file_path=file_path, format_name=format_name),
                          timeout=WEBAPI_RESOURCE_TIMEOUT)

    def plot_workspace_resource(self, base_dir: str, res_name: str,
                                var_name: str = None, file_path: str = None,
                                monitor: Monitor = Monitor.NONE) -> None:
        url = self._url('/ws/res/plot/{base_dir}/{res_name}',
                        path_args=dict(base_dir=base_dir, res_name=res_name),
                        query_args=self._query(var_name=var_name, file_path=file_path))
        self._fetch_json(url, timeout=WEBAPI_RESOURCE_TIMEOUT + WEBAPI_PLOT_TIMEOUT)

    def print_workspace_resource(self, base_dir: str, res_name_or_expr: str = None,
                                 monitor: Monitor = Monitor.NONE) -> None:
        self._ws_json_rpc("print_workspace_resource",
                          dict(base_dir=base_dir, res_name_or_expr=res_name_or_expr),
                          timeout=WEBAPI_RESOURCE_TIMEOUT,
                          monitor=monitor)


class WebSocketClient(object):
    def __init__(self, url, timeout):
        self.url = url
        self.timeout = timeout
        self.ioloop = ioloop.IOLoop.instance()
        self.json_rpc_request = None
        self.monitor = None
        self.ws = None
        self.id = 1

    def connect(self):
        self.ioloop.run_sync(self._connect, timeout=5)

    def invokeMethod(self, method, params, timeout, monitor: Monitor):
        self.json_rpc_request = self._format_rpc_request(method, params)
        self.monitor = monitor
        return self.ioloop.run_sync(self._invokeMethod, timeout=timeout)

    def close(self):
        if self.ws:
            self.ws.close()

    @gen.coroutine
    def _connect(self):
        try:
            self.ws = yield websocket.websocket_connect(self.url, self.ioloop)
        except Exception as e:
            print("connection error")
            raise e

    @gen.coroutine
    def _invokeMethod(self):
        self.ws.write_message(self.json_rpc_request)
        work_reported = 0
        while True:
            response = yield self.ws.read_message()
            json_response = json.loads(response)
            if 'progress' in json_response and self.monitor:
                progress = json_response['progress']
                print(progress)
                if 'message' in progress:
                    message = progress['message']
                    if message == 'Started':
                        total = progress.get('total', 100)
                        label = progress.get('label', '')
                        self.monitor.start(label, total)
                    elif message == 'Done':
                        self.monitor.done()
                    else:
                        worked = progress.get('worked', 0)
                        msg = progress.get('message', '')
                        self.monitor.progress(worked - work_reported, msg)
                        work_reported = worked
            else:
                return response

    def _format_rpc_request(self, method, params):
        id = self.id
        self.id += 1
        json_rpc_request = dict(jsonrpc='2.0',
                                id=id,
                                method=method,
                                params=params)
        return json.dumps(json_rpc_request)
