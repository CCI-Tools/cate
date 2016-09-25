# The MIT License (MIT)
# Copyright (c) 2016 by the ECT Development Team and contributors
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
import os
import shutil
import sys
import urllib.parse
import urllib.request
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from typing import List

from ect.core.monitor import Monitor
from ect.core.objectio import write_object
from ect.core.op import OP_REGISTRY
from ect.core.op import OpMetaInfo, parse_op_args
from ect.core.util import Namespace, encode_url_path
from ect.core.workflow import Workflow, OpStep, NodePort

WORKSPACE_DATA_DIR_NAME = '.ect-workspace'
WORKSPACE_WORKFLOW_FILE_NAME = 'workflow.json'


# TODO (forman, 20160908): implement file lock for workspaces in access


class WorkspaceError(Exception):
    def __init__(self, cause, *args, **kwargs):
        if isinstance(cause, Exception):
            super(WorkspaceError, self).__init__(str(cause), *args, **kwargs)
            _, _, traceback = sys.exc_info()
            self.with_traceback(traceback)
        elif isinstance(cause, str):
            super(WorkspaceError, self).__init__(cause, *args, **kwargs)
        else:
            super(WorkspaceError, self).__init__(*args, **kwargs)
        self._cause = cause

    @property
    def cause(self):
        return self._cause


class Workspace:
    """
    A Workspace uses a :py:class:`Workspace` to record user operations.
    By design, workspace workflows have no inputs and every step is an output.
    """

    def __init__(self, base_dir: str, workflow: Workflow):
        assert base_dir
        assert workflow
        self._base_dir = base_dir
        self._workflow = workflow
        self._resource_values = dict()

    def __del__(self):
        self.close()

    @property
    def base_dir(self) -> str:
        """The Workspace's workflow."""
        return self._base_dir

    @property
    def workflow(self) -> Workflow:
        """The Workspace's workflow."""
        return self._workflow

    @property
    def workspace_dir(self) -> str:
        return self.get_workspace_dir(self.base_dir)

    @property
    def workflow_file(self) -> str:
        return self.get_workflow_file(self.base_dir)

    @classmethod
    def get_workspace_dir(cls, base_dir) -> str:
        return os.path.join(base_dir, WORKSPACE_DATA_DIR_NAME)

    @classmethod
    def get_workflow_file(cls, base_dir) -> str:
        return os.path.join(cls.get_workspace_dir(base_dir), WORKSPACE_WORKFLOW_FILE_NAME)

    @classmethod
    def new_workflow(cls, header_dict: dict = None) -> Workflow:
        return Workflow(OpMetaInfo('workspace_workflow',
                                   has_monitor=True,
                                   header_dict=header_dict or {}))

    @classmethod
    def create(cls, base_dir: str, description: str = None) -> 'Workspace':
        try:
            if not os.path.isdir(base_dir):
                os.mkdir(base_dir)

            workspace_dir = cls.get_workspace_dir(base_dir)
            workflow_file = cls.get_workflow_file(base_dir)
            if not os.path.isdir(workspace_dir):
                os.mkdir(workspace_dir)
            elif os.path.isfile(workflow_file):
                raise WorkspaceError('workspace exists: %s' % base_dir)

            workflow = Workspace.new_workflow(dict(description=description or ''))
            workflow.store(workflow_file)
            return Workspace(base_dir, workflow)
        except (IOError, OSError, FileExistsError) as e:
            raise WorkspaceError(e)

    @classmethod
    def load(cls, base_dir: str) -> 'Workspace':
        if not os.path.isdir(cls.get_workspace_dir(base_dir)):
            raise WindowsError('not a valid workspace: %s' % base_dir)
        try:
            workflow_file = cls.get_workflow_file(base_dir)
            workflow = Workflow.load(workflow_file)
            return Workspace(base_dir, workflow)
        except (IOError, OSError) as e:
            raise WorkspaceError(e)

    def store(self):
        try:
            self.workflow.store(self.workflow_file)
        except (IOError, OSError) as e:
            raise WorkspaceError(e)

    def close(self):
        resource_cache = self._resource_values
        self._resource_values = dict()
        for value in resource_cache.values():
            Workspace.close_resource_value(value)

    def delete(self):
        try:
            shutil.rmtree(self.workspace_dir)
        except (IOError, OSError) as e:
            raise WorkspaceError(e)
        self.close()

    def execute_workflow(self, res_name, monitor):
        result = self.workflow(monitor=monitor)
        if res_name in result:
            obj = result[res_name]
        else:
            obj = result
        return obj

    def get_resource_value(self, resource_name: str, default: object = None) -> object:
        return self._resource_values.get(resource_name, default=default)

    def add_resource_value(self, resource_name: str, value: object) -> None:
        old_value = self.get_resource_value(resource_name, default=None)
        if value is old_value:
            return
        self._resource_values[resource_name] = value
        self.close_resource_value(old_value)

    def remove_resource_value(self, resource_name: str) -> None:
        value = self._resource_values.pop(resource_name, None)
        self.close_resource_value(value)

    @classmethod
    def close_resource_value(cls, value: object) -> None:
        if value is None:
            return
        # noinspection PyBroadException
        try:
            # noinspection PyUnresolvedReferences
            value.close()
        except:
            pass

    @classmethod
    def from_json_dict(cls, json_dict):
        base_dir = json_dict.get('base_dir', None)
        workflow_json = json_dict.get('workflow', None)
        workflow = Workflow.from_json_dict(workflow_json)
        return Workspace(base_dir, workflow)

    def to_json_dict(self):
        return OrderedDict([('base_dir', self.base_dir),
                            ('workflow', self.workflow.to_json_dict())])

    def set_resource(self, res_name: str, op_name: str, op_args: List[str], can_exist=False, validate_args=False):
        assert res_name
        assert op_name
        assert op_args

        op = OP_REGISTRY.get_op(op_name)
        if not op:
            raise WorkspaceError("unknown operation '%s'" % op_name)

        op_step = OpStep(op, node_id=res_name)

        workflow = self.workflow

        # This namespace will allow us to wire the new resource with existing workflow steps
        # We only add step outputs, so we cannot reference another step's input neither.
        # Note that workspace workflows never have any inputs to be referenced anyway.
        namespace = dict()
        for step in workflow.steps:
            output_namespace = step.output
            namespace[step.id] = output_namespace

        does_exist = res_name in namespace
        if not can_exist and does_exist:
            raise WorkspaceError("resource '%s' already exists" % res_name)

        if does_exist:
            # Prevent resource from self-referencing
            namespace.pop(res_name, None)

        raw_op_args = op_args
        try:
            # some arguments may now be of type 'Namespace' or 'NodePort', which are outputs of other workflow steps
            op_args, op_kwargs = parse_op_args(raw_op_args, namespace=namespace)
        except ValueError as e:
            raise WorkspaceError(e)

        if op_args:
            raise WorkspaceError("positional arguments are not yet supported")

        if validate_args:
            # validate the op_kwargs using the new operation's meta-info
            namespace_types = set(type(value) for value in namespace.values())
            op_step.op_meta_info.validate_input_values(op_kwargs, except_types=namespace_types)

        return_output_name = OpMetaInfo.RETURN_OUTPUT_NAME

        # Wire new op_step with outputs from existing steps
        for input_name, input_value in op_kwargs.items():
            if input_name not in op_step.input:
                raise WorkspaceError("'%s' is not an input of operation '%s'" % (input_name, op_name))
            input_port = op_step.input[input_name]
            if isinstance(input_value, NodePort):
                # input_value is an output NodePort of another step
                input_port.source = input_value
            elif isinstance(input_value, Namespace):
                # input_value is output_namespace of another step
                if return_output_name not in input_value:
                    raise WorkspaceError("illegal value for input '%s'" % input_name)
                input_port.source = input_value['return']
            else:
                # Neither a Namespace nor a NodePort, it must be a constant value
                input_port.value = input_value

        workflow = self._workflow
        # noinspection PyUnusedLocal
        old_step = workflow.add_step(op_step, can_exist=True)
        if does_exist:
            # If the step already existed before, we must resolve source references again
            workflow.resolve_source_refs()
            for workflow_output_port in workflow.output[:]:
                if workflow_output_port.source.node is old_step:
                    # We set old outputs to None, as we don't want to change output order and don't want to delete old
                    # output ports. However, we should better do this one day.
                    workflow_output_port.value = None

        # Remove any cached resource value
        self.remove_resource_value(res_name)
        # TODO (forman, 20160924): Must also remove all cached resources values that depend on old_step, if any

        if op_step.op_meta_info.has_named_outputs:
            for step_output_port in op_step.output[:]:
                workflow_output_port_name = res_name + '$' + step_output_port.name
                workflow.op_meta_info.output[workflow_output_port_name] = \
                    op_step.op_meta_info.output[step_output_port.name]
                workflow_output_port = NodePort(workflow, workflow_output_port_name)
                workflow_output_port.source = step_output_port
                workflow.output[workflow_output_port.name] = workflow_output_port
        else:
            workflow.op_meta_info.output[res_name] = op_step.op_meta_info.output[return_output_name]
            workflow_output_port = NodePort(workflow, res_name)
            workflow_output_port.source = op_step.output[return_output_name]
            workflow.output[workflow_output_port.name] = workflow_output_port


class WorkspaceManager(metaclass=ABCMeta):
    @abstractmethod
    def get_workspace(self, base_dir: str, open: bool = False) -> Workspace:
        pass

    @abstractmethod
    def new_workspace(self, base_dir: str, description: str = None) -> Workspace:
        pass

    @abstractmethod
    def open_workspace(self, base_dir: str) -> Workspace:
        pass

    @abstractmethod
    def close_workspace(self, base_dir: str) -> None:
        pass

    @abstractmethod
    def close_all_workspaces(self) -> None:
        pass

    @abstractmethod
    def delete_workspace(self, base_dir: str) -> None:
        pass

    @abstractmethod
    def clean_workspace(self, base_dir: str) -> None:
        pass

    @abstractmethod
    def set_workspace_resource(self, base_dir: str, res_name: str,
                               op_name: str, op_args: List[str]) -> None:
        pass

    @abstractmethod
    def write_workspace_resource(self, base_dir: str, res_name: str,
                                 file_path: str, format_name: str = None,
                                 monitor: Monitor = Monitor.NULL) -> None:
        pass

    @abstractmethod
    def plot_workspace_resource(self, base_dir: str, res_name: str,
                                var_name: str = None, file_path: str = None,
                                monitor: Monitor = Monitor.NULL) -> None:
        pass


class FSWorkspaceManager(WorkspaceManager):
    def __init__(self, resolve_dir: str = None):
        self._open_workspaces = dict()
        self._resolve_dir = os.path.abspath(resolve_dir or os.curdir)

    def resolve_path(self, dir_path):
        if dir_path and os.path.isabs(dir_path):
            return os.path.normpath(dir_path)
        return os.path.abspath(os.path.join(self._resolve_dir, dir_path or ''))

    def get_workspace(self, base_dir: str, open: bool = False) -> Workspace:
        base_dir = self.resolve_path(base_dir)
        workspace = self._open_workspaces.get(base_dir, None)
        if workspace:
            return workspace
        workspace = Workspace.load(base_dir)
        if open:
            assert base_dir not in self._open_workspaces
            self._open_workspaces[base_dir] = workspace
        return workspace

    def new_workspace(self, base_dir: str, description: str = None) -> Workspace:
        base_dir = self.resolve_path(base_dir)
        workspace_dir = Workspace.get_workspace_dir(base_dir)
        if os.path.isdir(workspace_dir):
            raise WorkspaceError('workspace exists: %s' % base_dir)
        workspace = Workspace.create(base_dir, description=description)
        assert base_dir not in self._open_workspaces
        self._open_workspaces[base_dir] = workspace
        return workspace

    def open_workspace(self, base_dir: str) -> Workspace:
        return self.get_workspace(base_dir, open=True)

    def close_workspace(self, base_dir: str) -> None:
        base_dir = self.resolve_path(base_dir)
        workspace = self._open_workspaces.pop(base_dir, None)
        if workspace is not None:
            workspace.close()

    def close_all_workspaces(self) -> None:
        workspaces = self._open_workspaces.values()
        self._open_workspaces = dict()
        for workspace in workspaces:
            workspace.close()

    def clean_workspace(self, base_dir: str) -> None:
        base_dir = self.resolve_path(base_dir)
        # noinspection PyBroadException
        try:
            workspace = Workspace.load(base_dir)
        except:
            workspace = None
        workflow_file = Workspace.get_workflow_file(base_dir)
        if os.path.isfile(workflow_file):
            try:
                os.remove(workflow_file)
            except (IOError, OSError) as e:
                raise WorkspaceError(e)
        # Create new workflow but keep old header info
        workflow = Workspace.new_workflow(header_dict=workspace.workflow.op_meta_info.header if workspace else None)
        workspace = Workspace(base_dir, workflow)
        self._open_workspaces[base_dir] = workspace
        workspace.store()

    def delete_workspace(self, base_dir: str) -> None:
        base_dir = self.resolve_path(base_dir)
        workspace_dir = Workspace.get_workspace_dir(base_dir)
        if not os.path.isdir(workspace_dir):
            raise WorkspaceError('not a workspace: %s' % base_dir)
        try:
            shutil.rmtree(workspace_dir)
        except (IOError, OSError) as e:
            raise WorkspaceError(e)
        if base_dir in self._open_workspaces:
            del self._open_workspaces[base_dir]

    def set_workspace_resource(self, base_dir: str, res_name: str, op_name: str, op_args: List[str]) -> None:
        workspace = self.get_workspace(base_dir)
        workspace.set_resource(res_name, op_name, op_args, can_exist=True, validate_args=True)
        workspace.store()

    def write_workspace_resource(self, base_dir: str, res_name: str,
                                 file_path: str, format_name: str = None,
                                 monitor: Monitor = Monitor.NULL) -> None:
        # TBD: shall we add a new step to the workflow or just execute the workflow,
        # then write the desired resource?
        workspace = self.get_workspace(base_dir)
        with monitor.starting('Writing resource "%s"' % res_name, total_work=10):
            obj = workspace.execute_workflow(res_name, monitor.child(9))
            write_object(obj, file_path, format_name=format_name)
            monitor.progress(work=1, msg='Writing file %s' % file_path)

    def plot_workspace_resource(self, base_dir: str, res_name: str,
                                var_name: str = None, file_path: str = None,
                                monitor: Monitor = Monitor.NULL) -> None:
        # TBD: shall we add a new step to the workflow or just execute the workflow,
        # then write the desired resource?
        workspace = self.get_workspace(base_dir)
        obj = workspace.execute_workflow(res_name, monitor)
        import xarray as xr
        import numpy as np
        import matplotlib.pyplot as plt

        if isinstance(obj, xr.Dataset):
            ds = obj
            if var_name:
                variables = [ds.data_vars[var_name]]
            else:
                variables = ds.data_vars.values()
            for var in variables:
                if hasattr(var, 'plot'):
                    print('Plotting ', var)
                    var.plot()
            plt.show()
        elif isinstance(obj, xr.DataArray):
            var = obj
            if hasattr(var, 'plot'):
                print('Plotting ', var)
                var.plot()
                plt.show()
        elif isinstance(obj, np.ndarray):
            plt.plot(obj)
            plt.show()
        else:
            raise WorkspaceError("don't know how to plot a \"%s\"" % type(obj))


class WebAPIWorkspaceManager(WorkspaceManager):
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
        status = json_response.get('status', None)
        if status == 'error':
            error_details = json_response.get('error')
            message = error_details.get('message', None) if error_details else None
            type_name = error_details.get('type', None) if error_details else None
            raise error_type(message or type_name)
        return json_response.get('content', None)

    def is_running(self, timeout: float = None) -> bool:
        # noinspection PyBroadException
        try:
            self._fetch_json('/', timeout=timeout)
            return True
        except WorkspaceError:
            return True
        except:
            return False

    def get_workspace(self, base_dir: str, open: bool = False) -> Workspace:
        url = self._url('/ws/get/{base_dir}',
                        path_args=dict(base_dir=base_dir),
                        query_args=dict(open=open))
        json_dict = self._fetch_json(url)
        return Workspace.from_json_dict(json_dict)

    def new_workspace(self, base_dir: str, description: str = None) -> Workspace:
        url = self._url('/ws/new', query_args=dict(base_dir=base_dir, description=description or ''))
        json_dict = self._fetch_json(url)
        return Workspace.from_json_dict(json_dict)

    def open_workspace(self, base_dir: str) -> Workspace:
        url = self._url('/ws/open/{base_dir}', path_args=dict(base_dir=base_dir))
        json_dict = self._fetch_json(url)
        return Workspace.from_json_dict(json_dict)

    def close_workspace(self, base_dir: str) -> None:
        url = self._url('/ws/close/{base_dir}', path_args=dict(base_dir=base_dir))
        self._fetch_json(url)

    def close_all_workspaces(self) -> None:
        url = self._url('/ws/close_all')
        self._fetch_json(url)

    def delete_workspace(self, base_dir: str) -> None:
        url = self._url('/ws/del/{base_dir}', path_args=dict(base_dir=base_dir))
        self._fetch_json(url)

    def clean_workspace(self, base_dir: str) -> None:
        url = self._url('/ws/clean/{base_dir}', path_args=dict(base_dir=base_dir))
        self._fetch_json(url)

    def set_workspace_resource(self, base_dir: str, res_name: str, op_name: str, op_args: List[str]) -> None:
        url = self._url('/ws/res/set/{base_dir}/{res_name}',
                        path_args=dict(base_dir=base_dir, res_name=res_name))
        data = urllib.parse.urlencode(dict(op_name=op_name, op_args=json.dumps(op_args)))
        self._fetch_json(url, data=data.encode())

    def write_workspace_resource(self, base_dir: str, res_name: str,
                                 file_path: str, format_name: str = None,
                                 monitor: Monitor = Monitor.NULL) -> None:
        url = self._url('/ws/res/write/{base_dir}/{res_name}',
                        path_args=dict(base_dir=base_dir, res_name=res_name))
        data = urllib.parse.urlencode(dict(file_path=file_path, format_name=format_name))
        self._fetch_json(url, data=data.encode())

    def plot_workspace_resource(self, base_dir: str, res_name: str,
                                var_name: str = None, file_path: str = None,
                                monitor: Monitor = Monitor.NULL) -> None:
        url = self._url('/ws/res/plot/{base_dir}/{res_name}',
                        path_args=dict(base_dir=base_dir, res_name=res_name))
        data = urllib.parse.urlencode(dict(var_name=var_name, file_path=file_path))
        self._fetch_json(url, data=data.encode())
