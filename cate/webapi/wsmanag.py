# The MIT License (MIT)
# Copyright (c) 2018 by the ESA CCI Toolbox development team and contributors
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


import json
import urllib.parse
import urllib.request
from typing import List, Tuple, Optional, Any, Union

from tornado import gen, ioloop, websocket

from cate.conf.defaults import WEBAPI_WORKSPACE_TIMEOUT, WEBAPI_RESOURCE_TIMEOUT, WEBAPI_PLOT_TIMEOUT
from cate.core.workspace import Workspace, OpKwArgs
from cate.core.wsmanag import WorkspaceManager
from cate.util.misc import encode_url_path
from cate.util.monitor import Monitor
from cate.util.safe import safe_eval
from cate.util.web.serviceinfo import join_address_and_port

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"


class WebAPIWorkspaceManager(WorkspaceManager):
    """
    Implementation of the WorkspaceManager interface against a WebSocket using a JSON RPC protocol.
    """

    def __init__(self, service_info: dict, conn_timeout: float = 5, rpc_timeout: float = 120):
        address = service_info.get('address', None) or 'localhost'
        port = service_info.get('port', None)
        if not port:
            raise ValueError('missing "port" number in service_info argument')
        self.base_url = f'http://{join_address_and_port(address, port)}'
        self.ws_url = f'ws://{join_address_and_port(address, port)}/api'
        self.ws_client = WebSocketClient(self.ws_url)
        self.ws_client.connect(conn_timeout)
        self.rpc_timeout = rpc_timeout

    def _url(self, path_pattern: str, path_args: dict = None, query_args: dict = None) -> str:
        return self.base_url + encode_url_path(path_pattern, path_args=path_args, query_args=query_args)

    def _invoke_method(self, method, params, timeout: float = None,
                       monitor: Monitor = Monitor.NONE):
        rpc_response = self.ws_client.invoke_method(method, params, timeout=timeout, monitor=monitor)
        error_info = rpc_response.get('error')
        if error_info:
            WebAPIWorkspaceManager._raise_error(error_info)
        return rpc_response.get('response')

    def _fetch_json(self, url, data=None, timeout: float = None):
        with urllib.request.urlopen(url, data=data, timeout=timeout or self.rpc_timeout) as response:
            json_text = response.read()
        json_response = json.loads(json_text.decode('utf-8'))
        status = json_response.get('status')
        if status == 'error':
            WebAPIWorkspaceManager._raise_error(json_response.get('error'))
        return json_response.get('content')

    @staticmethod
    def _raise_error(error_info):
        exc_type = None
        if error_info:
            message = error_info.get('message') or ''
            error_ex_info = error_info.get('data')
            if error_ex_info:
                exc_type_name = error_ex_info.get('exception')
                if exc_type_name:
                    # noinspection PyBroadException
                    try:
                        exc_type = safe_eval(exc_type_name)
                    except Exception:
                        pass
                # TODO (forman): find out how can we preserve traceback without adding it to the message string
                # tb = error_ex_info.get('traceback')
        else:
            message = 'Unknown error from WebAPI service.'

        exc = None
        if exc_type:
            # noinspection PyBroadException
            try:
                exc = exc_type(message)
            except Exception:
                pass

        if exc is None:
            exc = RuntimeError(message)

        raise exc

    # noinspection PyMethodMayBeStatic
    def _query(self, **kwargs):
        return {key: value for key, value in kwargs.items() if value is not None}

    def _post_data(self, **kwargs):
        data = urllib.parse.urlencode(self._query(**kwargs))
        return data.encode() if data else None

    @classmethod
    def get_traceback_header(cls) -> str:
        traceback_title = 'Cate WebAPI service traceback'
        traceback_line = len(traceback_title) * '='
        return '\n' + traceback_line + '\n' + traceback_title + '\n' + traceback_line + '\n'

    @property
    def root_path(self) -> Optional[str]:
        return None

    def resolve_path(self, path: str) -> str:
        return path

    def resolve_workspace_dir(self, path_or_name: str) -> str:
        return path_or_name

    def get_open_workspaces(self) -> List[Workspace]:
        json_list = self._invoke_method("get_open_workspaces", dict(), timeout=WEBAPI_WORKSPACE_TIMEOUT)
        return [Workspace.from_json_dict(ws_json_dict) for ws_json_dict in json_list]

    def list_workspace_names(self) -> List[str]:
        json_list = self._invoke_method("list_workspace_names", dict(), timeout=WEBAPI_WORKSPACE_TIMEOUT)
        return json_list

    def get_workspace(self, base_dir: str) -> Workspace:
        json_dict = self._invoke_method("get_workspace", dict(base_dir=base_dir), timeout=WEBAPI_WORKSPACE_TIMEOUT)
        return Workspace.from_json_dict(json_dict)

    def new_workspace(self, base_dir: str, description: str = None) -> Workspace:
        json_dict = self._invoke_method("new_workspace", dict(base_dir=base_dir, description=description),
                                        timeout=WEBAPI_WORKSPACE_TIMEOUT)
        return Workspace.from_json_dict(json_dict)

    def open_workspace(self, base_dir: str, monitor: Monitor = Monitor.NONE) -> Workspace:
        json_dict = self._invoke_method("open_workspace", dict(base_dir=base_dir),
                                        timeout=WEBAPI_WORKSPACE_TIMEOUT,
                                        monitor=monitor)
        return Workspace.from_json_dict(json_dict)

    def close_workspace(self, base_dir: str) -> None:
        self._invoke_method("close_workspace", dict(base_dir=base_dir), timeout=WEBAPI_WORKSPACE_TIMEOUT)

    def close_all_workspaces(self) -> None:
        self._invoke_method("close_all_workspaces", dict(), timeout=WEBAPI_WORKSPACE_TIMEOUT)

    def save_workspace_as(self, base_dir: str, to_dir: str, monitor: Monitor = Monitor.NONE) -> Workspace:
        json_dict = self._invoke_method("save_workspace_as",
                                        dict(base_dir=base_dir, to_dir=to_dir),
                                        timeout=WEBAPI_WORKSPACE_TIMEOUT,
                                        monitor=monitor)
        return Workspace.from_json_dict(json_dict)

    def save_workspace(self, base_dir: str, monitor: Monitor = Monitor.NONE) -> Workspace:
        json_dict = self._invoke_method("save_workspace", dict(base_dir=base_dir),
                                        timeout=WEBAPI_WORKSPACE_TIMEOUT,
                                        monitor=monitor)
        return Workspace.from_json_dict(json_dict)

    def save_all_workspaces(self, monitor: Monitor = Monitor.NONE) -> None:
        self._invoke_method("save_all_workspaces", dict(), timeout=WEBAPI_WORKSPACE_TIMEOUT, monitor=monitor)

    def delete_workspace(self, base_dir: str, remove_completely: bool = False) -> None:
        self._invoke_method("delete_workspace",
                            dict(base_dir=base_dir, remove_completely=remove_completely),
                            timeout=WEBAPI_WORKSPACE_TIMEOUT)

    def clean_workspace(self, base_dir: str) -> Workspace:
        json_dict = self._invoke_method("clean_workspace", dict(base_dir=base_dir), timeout=WEBAPI_WORKSPACE_TIMEOUT)
        return Workspace.from_json_dict(json_dict)

    def run_op_in_workspace(self, base_dir: str, op_name: str, op_args: OpKwArgs,
                            monitor: Monitor = Monitor.NONE) -> Union[Any, None]:
        return self._invoke_method("run_op_in_workspace",
                                   dict(base_dir=base_dir, op_name=op_name, op_args=op_args),
                                   timeout=WEBAPI_WORKSPACE_TIMEOUT,
                                   monitor=monitor)

    def delete_workspace_resource(self, base_dir: str, res_name: str) -> Workspace:
        json_dict = self._invoke_method("delete_workspace_resource",
                                        dict(base_dir=base_dir, res_name=res_name),
                                        timeout=WEBAPI_RESOURCE_TIMEOUT)
        return Workspace.from_json_dict(json_dict)

    def set_workspace_resource_persistence(self, base_dir: str, res_name: str, persistent: bool) -> Workspace:
        json_dict = self._invoke_method("set_workspace_resource_persistence",
                                        dict(base_dir=base_dir, res_name=res_name, persistent=persistent),
                                        timeout=WEBAPI_RESOURCE_TIMEOUT)
        return Workspace.from_json_dict(json_dict)

    def set_workspace_resource(self,
                               base_dir: str,
                               op_name: str,
                               op_args: OpKwArgs,
                               res_name: Optional[str] = None,
                               overwrite: bool = False,
                               monitor: Monitor = Monitor.NONE) -> Tuple[Workspace, str]:
        json_list = self._invoke_method("set_workspace_resource",
                                        dict(base_dir=base_dir, res_name=res_name, op_name=op_name,
                                             op_args=op_args, overwrite=overwrite),
                                        timeout=WEBAPI_RESOURCE_TIMEOUT,
                                        monitor=monitor)
        return Workspace.from_json_dict(json_list[0]), json_list[1]

    def rename_workspace_resource(self, base_dir: str,
                                  res_name: str, new_res_name: str) -> Workspace:
        json_dict = self._invoke_method("rename_workspace_resource",
                                        dict(base_dir=base_dir, res_name=res_name, new_res_name=new_res_name),
                                        timeout=WEBAPI_RESOURCE_TIMEOUT)
        return Workspace.from_json_dict(json_dict)

    def write_workspace_resource(self, base_dir: str, res_name: str,
                                 file_path: str, format_name: str = None,
                                 monitor: Monitor = Monitor.NONE) -> None:
        self._invoke_method("write_workspace_resource",
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
        self._invoke_method("print_workspace_resource",
                            dict(base_dir=base_dir, res_name_or_expr=res_name_or_expr),
                            timeout=WEBAPI_RESOURCE_TIMEOUT,
                            monitor=monitor)

    def _create_scratch_dir(self, scratch_dir_name: str) -> str:
        return ''

    def _resolve_target_path(self, target_dir: str) -> str:
        return ''


class WebSocketClient(object):
    def __init__(self, url):
        self.url = url
        self.connection = None
        self.current_method_id = 1

    def connect(self, timeout: float):
        ioloop.IOLoop.current().run_sync(self._connect, timeout=timeout)

    def invoke_method(self, method, params, timeout, monitor: Monitor) -> dict:
        json_rpc_request = self._new_rpc_request(method, params)

        def do_json_rpc() -> dict:
            return _do_json_rpc(self.connection, json_rpc_request, monitor)

        return ioloop.IOLoop.current().run_sync(do_json_rpc, timeout=timeout)

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    @gen.coroutine
    def _connect(self):
        self.connection = yield websocket.websocket_connect(self.url)

    def _new_rpc_request(self, method_name, method_params) -> dict:
        return dict(jsonrpc='2.0',
                    id=self._new_method_id(),
                    method=method_name,
                    params=method_params)

    def _new_method_id(self) -> int:
        new_method_id = self.current_method_id
        self.current_method_id += 1
        return new_method_id


@gen.coroutine
def _do_json_rpc(web_socket, rpc_request: dict, monitor: Monitor) -> dict:
    web_socket.write_message(json.dumps(rpc_request))
    work_reported = None
    started = False
    while True and (monitor is None or not monitor.is_cancelled()):
        response_str = yield web_socket.read_message()
        rpc_response = json.loads(response_str)
        if 'progress' in rpc_response:
            if monitor:
                progress = rpc_response['progress']
                total = progress.get('total')
                label = progress.get('label')
                worked = progress.get('worked')
                msg = progress.get('message')

                if not started:
                    monitor.start(label or "start", total_work=total)
                    started = True

                if started:
                    if worked:
                        if work_reported is None:
                            work_reported = 0.0
                        work = worked - work_reported
                        work_reported = worked
                    else:
                        work = None
                    monitor.progress(work=work, msg=msg)
        else:
            if monitor and started:
                monitor.done()
            return rpc_response

    return {}
