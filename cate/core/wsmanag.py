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

import os
import pprint
import shutil
import uuid
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from typing import List, Union

from cate.conf.defaults import SCRATCH_WORKSPACES_PATH
from .objectio import write_object
from .workflow import Workflow
from .workspace import Workspace, WorkspaceError
from ..util import UNDEFINED, Monitor


class WorkspaceManager(metaclass=ABCMeta):
    """
    Abstract base class which represents the ``WorkspaceManager`` interface.
    """

    @abstractmethod
    def get_open_workspaces(self) -> List[Workspace]:
        pass

    @abstractmethod
    def get_workspace(self, base_dir: str) -> Workspace:
        pass

    @abstractmethod
    def new_workspace(self, base_dir: Union[str, None], description: str = None) -> Workspace:
        pass

    @abstractmethod
    def open_workspace(self, base_dir: str, monitor: Monitor = Monitor.NONE) -> Workspace:
        pass

    @abstractmethod
    def close_workspace(self, base_dir: str) -> None:
        pass

    @abstractmethod
    def close_all_workspaces(self) -> None:
        pass

    @abstractmethod
    def save_workspace_as(self, base_dir: str, to_dir: str,
                          monitor: Monitor = Monitor.NONE) -> Workspace:
        pass

    @abstractmethod
    def save_workspace(self, base_dir: str) -> Workspace:
        pass

    @abstractmethod
    def save_all_workspaces(self, monitor: Monitor = Monitor.NONE) -> None:
        pass

    @abstractmethod
    def delete_workspace(self, base_dir: str) -> None:
        pass

    @abstractmethod
    def clean_workspace(self, base_dir: str) -> None:
        pass

    @abstractmethod
    def run_op_in_workspace(self, base_dir: str,
                            op_name: str, op_args: List[str],
                            monitor: Monitor = Monitor.NONE) -> Workspace:
        pass

    @abstractmethod
    def set_workspace_resource(self, base_dir: str, res_name: str,
                               op_name: str, op_args: List[str],
                               monitor: Monitor = Monitor.NONE) -> Workspace:
        pass

    @abstractmethod
    def rename_workspace_resource(self, base_dir: str,
                                  res_name: str, new_res_name: str) -> Workspace:
        pass

    @abstractmethod
    def delete_workspace_resource(self, base_dir: str, res_name: str) -> Workspace:
        pass

    @abstractmethod
    def write_workspace_resource(self, base_dir: str, res_name: str,
                                 file_path: str, format_name: str = None,
                                 monitor: Monitor = Monitor.NONE) -> Workspace:
        pass

    @abstractmethod
    def plot_workspace_resource(self, base_dir: str, res_name: str,
                                var_name: str = None, file_path: str = None,
                                monitor: Monitor = Monitor.NONE) -> Workspace:
        pass

    @abstractmethod
    def print_workspace_resource(self, base_dir: str, res_name_or_expr: str = None,
                                 monitor: Monitor = Monitor.NONE) -> Workspace:
        pass


class FSWorkspaceManager(WorkspaceManager):
    # TODO (forman, 20160908): implement file lock for opened workspaces (issue #26)

    def __init__(self, resolve_dir: str = None):
        self._open_workspaces = OrderedDict()
        self._resolve_dir = os.path.abspath(resolve_dir or os.curdir)

    def num_open_workspaces(self) -> int:
        return len(self._open_workspaces)

    def resolve_path(self, dir_path):
        if dir_path and os.path.isabs(dir_path):
            return os.path.normpath(dir_path)
        return os.path.abspath(os.path.join(self._resolve_dir, dir_path or ''))

    def get_open_workspaces(self) -> List[Workspace]:
        return list(self._open_workspaces.values())

    def get_workspace(self, base_dir: str) -> Workspace:
        base_dir = self.resolve_path(base_dir)
        workspace = self._open_workspaces.get(base_dir, None)
        if workspace is None:
            raise WorkspaceError('workspace does not exist: ' + base_dir)
        assert not workspace.is_closed
        # noinspection PyTypeChecker
        return workspace

    def new_workspace(self, base_dir: str, description: str = None) -> Workspace:
        if base_dir is None:
            scratch_dir_name = str(uuid.uuid4())
            scratch_dir_path = os.path.join(SCRATCH_WORKSPACES_PATH, scratch_dir_name)
            os.makedirs(scratch_dir_path, exist_ok=True)
            base_dir = scratch_dir_path

        base_dir = self.resolve_path(base_dir)
        if base_dir in self._open_workspaces:
            raise WorkspaceError('workspace already opened: %s' % base_dir)
        workspace_dir = Workspace.get_workspace_dir(base_dir)
        if os.path.isdir(workspace_dir):
            raise WorkspaceError('workspace exists, consider opening it: %s' % base_dir)
        workspace = Workspace.create(base_dir, description=description)
        assert base_dir not in self._open_workspaces
        self._open_workspaces[base_dir] = workspace
        return workspace

    def open_workspace(self, base_dir: str, monitor: Monitor = Monitor.NONE) -> Workspace:
        base_dir = self.resolve_path(base_dir)
        workspace = self._open_workspaces.get(base_dir, None)
        if workspace is not None:
            assert not workspace.is_closed
            # noinspection PyTypeChecker
            return workspace
        workspace = Workspace.open(base_dir)
        assert base_dir not in self._open_workspaces
        workspace.execute_workflow(monitor=monitor)
        self._open_workspaces[base_dir] = workspace
        return workspace

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

    def save_workspace_as(self, base_dir: str, to_dir: str,
                          monitor: Monitor = Monitor.NONE) -> Workspace:
        base_dir = self.resolve_path(base_dir)
        workspace = self.get_workspace(base_dir)

        empty_dir_exists = False
        if os.path.realpath(base_dir) == os.path.realpath(to_dir):
            return workspace

        with monitor.starting('opening "%s"' % to_dir, 100):
            if os.path.exists(to_dir):
                if os.path.isdir(to_dir):
                    entries = list(os.listdir(to_dir))
                    monitor.progress(work=5)
                    if len(entries) == 0:
                        empty_dir_exists = True
                    else:
                        raise WorkspaceError('Directory is not empty: ' + to_dir)
                else:
                    raise WorkspaceError('A file with same name already exists: ' + to_dir)
            else:
                monitor.progress(work=5)

            # Save and close current workspace
            workspace.save()
            workspace.close()
            monitor.progress(work=5)

            try:
                # if the given directory exists and is empty, we must delete it because
                # shutil.copytree(base_dir, to_dir) expects to_dir to be non-existent
                if empty_dir_exists:
                    os.rmdir(to_dir)
                monitor.progress(work=5)
                # Copy all files to new location to_dir
                shutil.copytree(base_dir, to_dir)
                monitor.progress(work=10)
                # Reopen from new location
                new_workspace = self.open_workspace(to_dir, monitor=monitor.child(work=70))
                # If it was a scratch workspace, delete the original
                if workspace.is_scratch:
                    shutil.rmtree(base_dir)
                monitor.progress(work=5)
                return new_workspace
            except (IOError, OSError) as e:
                raise WorkspaceError(e)

    def save_workspace(self, base_dir: str) -> Workspace:
        base_dir = self.resolve_path(base_dir)
        workspace = self.get_workspace(base_dir)
        if workspace:
            workspace.save()
        return workspace

    def save_all_workspaces(self, monitor: Monitor = Monitor.NONE) -> None:
        workspaces = self._open_workspaces.values()
        n = len(workspaces)
        with monitor.starting('Saving %s workspace(s)' % n, n):
            for workspace in workspaces:
                workspace.save()
                monitor.progress(work=1)

    def clean_workspace(self, base_dir: str) -> Workspace:
        base_dir = self.resolve_path(base_dir)
        workflow_file = Workspace.get_workflow_file(base_dir)
        old_workflow = None
        if os.path.isfile(workflow_file):
            # noinspection PyBroadException
            try:
                old_workflow = Workflow.load(workflow_file)
            except:
                pass
            try:
                os.remove(workflow_file)
            except (IOError, OSError) as e:
                raise WorkspaceError(e)
        old_workspace = self._open_workspaces.get(base_dir)
        if old_workspace:
            old_workspace.resource_cache.close()
        # Create new workflow but keep old header info
        workflow = Workspace.new_workflow(header_dict=old_workflow.op_meta_info.header if old_workflow else None)
        workspace = Workspace(base_dir, workflow)
        self._open_workspaces[base_dir] = workspace
        workspace.save()
        return workspace

    def delete_workspace(self, base_dir: str) -> None:
        self.close_workspace(base_dir)
        base_dir = self.resolve_path(base_dir)
        workspace_dir = Workspace.get_workspace_dir(base_dir)
        if not os.path.isdir(workspace_dir):
            raise WorkspaceError('not a workspace: %s' % base_dir)
        try:
            shutil.rmtree(workspace_dir)
        except (IOError, OSError) as e:
            raise WorkspaceError(e)

    def run_op_in_workspace(self, base_dir: str,
                            op_name: str, op_args: List[str],
                            monitor: Monitor = Monitor.NONE) -> Workspace:
        workspace = self.get_workspace(base_dir)
        workspace.run_op(op_name, op_args, validate_args=True, monitor=monitor)
        return workspace

    def set_workspace_resource(self, base_dir: str, res_name: str, op_name: str, op_args: List[str],
                               monitor: Monitor = Monitor.NONE) -> Workspace:
        workspace = self.get_workspace(base_dir)
        workspace.set_resource(res_name, op_name, op_args, overwrite=True, validate_args=True)
        workspace.execute_workflow(res_name=res_name, monitor=monitor)
        return workspace

    def rename_workspace_resource(self, base_dir: str,
                                  res_name: str, new_res_name: str) -> Workspace:
        workspace = self.get_workspace(base_dir)
        workspace.rename_resource(res_name, new_res_name)
        return workspace

    def delete_workspace_resource(self, base_dir: str, res_name: str) -> Workspace:
        workspace = self.get_workspace(base_dir)
        workspace.delete_resource(res_name)
        return workspace

    def write_workspace_resource(self, base_dir: str, res_name: str,
                                 file_path: str, format_name: str = None,
                                 monitor: Monitor = Monitor.NONE) -> Workspace:
        workspace = self.get_workspace(base_dir)
        with monitor.starting('Writing resource "%s"' % res_name, total_work=10):
            obj = workspace.execute_workflow(res_name=res_name, monitor=monitor.child(work=9))
            write_object(obj, file_path, format_name=format_name)
            monitor.progress(work=1, msg='Writing file %s' % file_path)
        return workspace

    def plot_workspace_resource(self, base_dir: str, res_name: str,
                                var_name: str = None, file_path: str = None,
                                monitor: Monitor = Monitor.NONE) -> Workspace:
        workspace = self.get_workspace(base_dir)
        obj = self._get_resource_value(workspace, res_name, monitor)

        import xarray as xr
        import numpy as np
        import matplotlib
        matplotlib.use('Qt4Agg')
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
        return workspace

    def print_workspace_resource(self, base_dir: str, res_name_or_expr: str = None,
                                 monitor: Monitor = Monitor.NONE) -> Workspace:
        workspace = self.get_workspace(base_dir)
        value = self._get_resource_value(workspace, res_name_or_expr, monitor)
        pprint.pprint(value)
        return workspace

    # noinspection PyMethodMayBeStatic
    def _get_resource_value(self, workspace, res_name_or_expr, monitor):
        value = UNDEFINED
        if res_name_or_expr is None:
            value = workspace.resource_cache
        elif res_name_or_expr.isidentifier() and workspace.workflow.find_node(res_name_or_expr) is not None:
            value = workspace.execute_workflow(res_name=res_name_or_expr, monitor=monitor)
        if value is UNDEFINED:
            value = eval(res_name_or_expr, None, workspace.resource_cache)
        return value
