# The MIT License (MIT)
# Copyright (c) 2017 by the Cate Development Team and contributors
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
from typing import List

from cate.conf.defaults import WEBAPI_WORKSPACE_TIMEOUT, WEBAPI_RESOURCE_TIMEOUT, WEBAPI_PLOT_TIMEOUT
from cate.core.workspace import Workspace, WorkspaceError
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
        self.timeout = timeout

    def _url(self, path_pattern: str, path_args: dict = None, query_args: dict = None) -> str:
        return self.base_url + encode_url_path(path_pattern, path_args=path_args, query_args=query_args)

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
        url = self._url('/ws/get_open')
        json_list = self._fetch_json(url, timeout=WEBAPI_WORKSPACE_TIMEOUT)
        return [Workspace.from_json_dict(ws_json_dict) for ws_json_dict in json_list]

    def get_workspace(self, base_dir: str) -> Workspace:
        url = self._url('/ws/get/{base_dir}',
                        path_args=dict(base_dir=base_dir))
        json_dict = self._fetch_json(url, timeout=WEBAPI_WORKSPACE_TIMEOUT)
        return Workspace.from_json_dict(json_dict)

    def new_workspace(self, base_dir: str, description: str = None) -> Workspace:
        url = self._url('/ws/new',
                        query_args=dict(base_dir=base_dir,
                                        description=description or ''))
        json_dict = self._fetch_json(url, timeout=WEBAPI_WORKSPACE_TIMEOUT)
        return Workspace.from_json_dict(json_dict)

    def open_workspace(self, base_dir: str, monitor: Monitor = Monitor.NONE) -> Workspace:
        url = self._url('/ws/open/{base_dir}',
                        path_args=dict(base_dir=base_dir))
        json_dict = self._fetch_json(url, timeout=WEBAPI_WORKSPACE_TIMEOUT)
        return Workspace.from_json_dict(json_dict)

    def close_workspace(self, base_dir: str) -> None:
        url = self._url('/ws/close/{base_dir}',
                        path_args=dict(base_dir=base_dir))
        self._fetch_json(url, timeout=WEBAPI_WORKSPACE_TIMEOUT)

    def close_all_workspaces(self) -> None:
        url = self._url('/ws/close_all')
        self._fetch_json(url, timeout=WEBAPI_WORKSPACE_TIMEOUT)

    def save_workspace_as(self, base_dir: str, to_dir: str, monitor: Monitor = Monitor.NONE) -> Workspace:
        url = self._url('/ws/save_as/{base_dir}',
                        path_args=dict(base_dir=base_dir), query_args=dict(to_dir=to_dir))
        json_dict = self._fetch_json(url, timeout=WEBAPI_WORKSPACE_TIMEOUT)
        return Workspace.from_json_dict(json_dict)

    def save_workspace(self, base_dir: str, monitor: Monitor = Monitor.NONE) -> Workspace:
        url = self._url('/ws/save/{base_dir}',
                        path_args=dict(base_dir=base_dir))
        json_dict = self._fetch_json(url, timeout=WEBAPI_WORKSPACE_TIMEOUT)
        return Workspace.from_json_dict(json_dict)

    def save_all_workspaces(self, monitor: Monitor = Monitor.NONE) -> None:
        url = self._url('/ws/save_all')
        self._fetch_json(url, timeout=WEBAPI_WORKSPACE_TIMEOUT)

    def delete_workspace(self, base_dir: str) -> None:
        url = self._url('/ws/del/{base_dir}',
                        path_args=dict(base_dir=base_dir))
        self._fetch_json(url, timeout=WEBAPI_WORKSPACE_TIMEOUT)

    def clean_workspace(self, base_dir: str) -> Workspace:
        url = self._url('/ws/clean/{base_dir}',
                        path_args=dict(base_dir=base_dir))
        json_dict = self._fetch_json(url, timeout=WEBAPI_WORKSPACE_TIMEOUT)
        return Workspace.from_json_dict(json_dict)

    def run_op_in_workspace(self, base_dir: str,
                            op_name: str, op_args: List[str],
                            monitor: Monitor = Monitor.NONE) -> Workspace:
        url = self._url('/ws/run_op/{base_dir}',
                        path_args=dict(base_dir=base_dir))
        json_dict = self._fetch_json(url, timeout=WEBAPI_RESOURCE_TIMEOUT,
                                     data=self._post_data(op_name=op_name, op_args=json.dumps(op_args)))
        return Workspace.from_json_dict(json_dict)

    def delete_workspace_resource(self, base_dir: str, res_name: str) -> Workspace:
        url = self._url('/ws/res/del/{base_dir}/{res_name}',
                        path_args=dict(base_dir=base_dir, res_name=res_name))
        json_dict = self._fetch_json(url, timeout=WEBAPI_RESOURCE_TIMEOUT)
        return Workspace.from_json_dict(json_dict)

    def set_workspace_resource(self, base_dir: str, res_name: str,
                               op_name: str, op_args: List[str],
                               monitor: Monitor = Monitor.NONE) -> Workspace:
        url = self._url('/ws/res/set/{base_dir}/{res_name}',
                        path_args=dict(base_dir=base_dir, res_name=res_name))
        json_dict = self._fetch_json(url, timeout=WEBAPI_RESOURCE_TIMEOUT,
                                     data=self._post_data(op_name=op_name, op_args=json.dumps(op_args)))
        return Workspace.from_json_dict(json_dict)

    def write_workspace_resource(self, base_dir: str, res_name: str,
                                 file_path: str, format_name: str = None,
                                 monitor: Monitor = Monitor.NONE) -> Workspace:
        url = self._url('/ws/res/write/{base_dir}/{res_name}',
                        path_args=dict(base_dir=base_dir, res_name=res_name),
                        query_args=self._query(file_path=file_path, format_name=format_name))
        json_dict = self._fetch_json(url, timeout=WEBAPI_RESOURCE_TIMEOUT)
        return Workspace.from_json_dict(json_dict)

    def plot_workspace_resource(self, base_dir: str, res_name: str,
                                var_name: str = None, file_path: str = None,
                                monitor: Monitor = Monitor.NONE) -> Workspace:
        url = self._url('/ws/res/plot/{base_dir}/{res_name}',
                        path_args=dict(base_dir=base_dir, res_name=res_name),
                        query_args=self._query(var_name=var_name, file_path=file_path))
        json_dict = self._fetch_json(url, timeout=WEBAPI_RESOURCE_TIMEOUT + WEBAPI_PLOT_TIMEOUT)
        return Workspace.from_json_dict(json_dict)

    def print_workspace_resource(self, base_dir: str, res_name_or_expr: str = None,
                                 monitor: Monitor = Monitor.NONE) -> Workspace:
        url = self._url('/ws/res/print/{base_dir}',
                        path_args=dict(base_dir=base_dir),
                        query_args=self._query(res_name_or_expr=res_name_or_expr))
        json_dict = self._fetch_json(url, timeout=WEBAPI_RESOURCE_TIMEOUT)
        return Workspace.from_json_dict(json_dict)
