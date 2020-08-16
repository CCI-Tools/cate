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

import logging
import os
import platform
import pprint
import shutil
import uuid
from abc import ABCMeta, abstractmethod
from typing import List, Union, Optional, Tuple, Any, Dict

from .objectio import write_object
from .workflow import Workflow
from .workspace import Workspace, OpKwArgs
from ..conf.defaults import DEFAULT_SCRATCH_WORKSPACES_PATH, WORKSPACE_DATA_DIR_NAME, WORKSPACES_DIR_NAME, \
    DEFAULT_WORKSPACES_PATH, SCRATCH_WORKSPACES_DIR_NAME
from ..core.types import ValidationError
from ..util.monitor import Monitor
from ..util.safe import safe_eval
from ..util.undefined import UNDEFINED

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

_LOG = logging.getLogger('cate')


class WorkspaceManager(metaclass=ABCMeta):
    """
    Abstract base class which represents the ``WorkspaceManager`` interface.
    """

    @property
    @abstractmethod
    def root_path(self) -> Optional[str]:
        pass

    @abstractmethod
    def resolve_path(self, path: str) -> str:
        pass

    @abstractmethod
    def get_open_workspaces(self) -> List[Workspace]:
        pass

    @abstractmethod
    def get_workspace(self, workspace_dir: str) -> Workspace:
        pass

    @abstractmethod
    def list_workspace_names(self) -> List[str]:
        pass

    @abstractmethod
    def new_workspace(self,
                      workspace_dir: Optional[str],
                      description: str = None) -> Workspace:
        pass

    @abstractmethod
    def open_workspace(self,
                       workspace_dir: str,
                       monitor: Monitor = Monitor.NONE) -> Workspace:
        pass

    @abstractmethod
    def close_workspace(self, workspace_dir: str) -> None:
        pass

    @abstractmethod
    def close_all_workspaces(self) -> None:
        pass

    @abstractmethod
    def save_workspace_as(self,
                          workspace_dir: str,
                          new_workspace_dir: str,
                          monitor: Monitor = Monitor.NONE) -> Workspace:
        pass

    @abstractmethod
    def save_workspace(self,
                       workspace_dir: str,
                       monitor: Monitor = Monitor.NONE) -> Workspace:
        pass

    @abstractmethod
    def save_all_workspaces(self, monitor: Monitor = Monitor.NONE) -> None:
        pass

    @abstractmethod
    def delete_workspace(self,
                         workspace_dir: str,
                         remove_completely: bool = False) -> None:
        pass

    @abstractmethod
    def clean_workspace(self, base_dir: str) -> Workspace:
        pass

    @abstractmethod
    def run_op_in_workspace(self,
                            workspace_dir: str,
                            op_name: str,
                            op_args: OpKwArgs,
                            monitor: Monitor = Monitor.NONE) -> Union[Any, None]:
        pass

    @abstractmethod
    def set_workspace_resource(self,
                               workspace_dir: str,
                               op_name: str,
                               op_args: OpKwArgs,
                               res_name: Optional[str] = None,
                               overwrite: bool = False,
                               monitor: Monitor = Monitor.NONE) -> Tuple[Workspace, str]:
        pass

    @abstractmethod
    def rename_workspace_resource(self,
                                  workspace_dir: str,
                                  res_name: str,
                                  new_res_name: str) -> Workspace:
        pass

    @abstractmethod
    def delete_workspace_resource(self,
                                  workspace_dir: str,
                                  res_name: str) -> Workspace:
        pass

    @abstractmethod
    def set_workspace_resource_persistence(self,
                                           workspace_dir: str,
                                           res_name: str,
                                           persistent: bool) -> Workspace:
        pass

    @abstractmethod
    def write_workspace_resource(self,
                                 workspace_dir: str,
                                 res_name: str,
                                 file_path: str,
                                 format_name: str = None,
                                 monitor: Monitor = Monitor.NONE) -> None:
        pass

    @abstractmethod
    def plot_workspace_resource(self,
                                workspace_dir: str,
                                res_name: str,
                                var_name: str = None,
                                file_path: str = None,
                                monitor: Monitor = Monitor.NONE) -> None:
        pass

    @abstractmethod
    def print_workspace_resource(self,
                                 workspace_dir: str,
                                 res_name_or_expr: str = None,
                                 monitor: Monitor = Monitor.NONE) -> None:
        pass


class FSWorkspaceManager(WorkspaceManager):

    def __init__(self, root_path: str = None):
        self._open_workspaces: Dict[str, Workspace] = {}
        self._root_path: Optional[str] = None
        if root_path:
            root_path = os.path.normpath(root_path)
            if not os.path.isabs(root_path):
                raise ValueError('root directory must be given as absolute path')
            if not os.path.isdir(root_path):
                raise ValueError('root directory does not exist')
            self._root_path = root_path

    @property
    def scratch_workspaces_dir_path(self) -> str:
        if self._root_path:
            return os.path.join(self._root_path, SCRATCH_WORKSPACES_DIR_NAME)
        return DEFAULT_SCRATCH_WORKSPACES_PATH

    @property
    def workspaces_dir(self) -> str:
        if self._root_path:
            return os.path.join(self._root_path, WORKSPACES_DIR_NAME)
        return DEFAULT_WORKSPACES_PATH

    @property
    def root_path(self) -> Optional[str]:
        return self._root_path

    def resolve_path(self, path: str) -> str:
        if self._root_path:
            if os.path.isabs(path):
                path = os.path.normpath(path)
                rel_path = os.path.relpath(path, self._root_path)
                if rel_path.startswith('..'):
                    raise ValueError(f'forbidden path: {path}')
            else:
                return os.path.join(self._root_path, path)
        return path

    def num_open_workspaces(self) -> int:
        return len(self._open_workspaces)

    def get_open_workspaces(self) -> List[Workspace]:
        return list(self._open_workspaces.values())

    def list_workspace_names(self) -> List[str]:
        workspaces_dir = self.workspaces_dir
        if not os.path.isdir(workspaces_dir):
            return []

        dir_list = []
        with os.scandir(workspaces_dir) as scan_list:
            for entry in scan_list:
                if entry.is_dir() \
                        and os.path.isdir(os.path.join(workspaces_dir, entry.name, WORKSPACE_DATA_DIR_NAME)):
                    dir_list.append(entry.name)

        return dir_list

    def get_workspace(self, workspace_dir: str) -> Workspace:
        workspace_dir = self.resolve_path(workspace_dir)
        workspace = self._open_workspaces.get(workspace_dir, None)
        if workspace is None:
            raise ValidationError('Workspace does not exist: %s' % workspace_dir)
        assert not workspace.is_closed
        # noinspection PyTypeChecker
        return workspace

    def new_workspace(self, workspace_dir: Optional[str], description: str = None) -> Workspace:
        is_scratch = False
        if workspace_dir is None:
            scratch_dir_name = str(uuid.uuid4())
            workspace_dir = os.path.join(self.scratch_workspaces_dir_path, scratch_dir_name)
            os.makedirs(workspace_dir, exist_ok=True)
            is_scratch = True
        elif not os.path.isabs(workspace_dir):
            # Just a name
            workspace_dir = os.path.join(self.workspaces_dir, workspace_dir)
        else:
            workspace_dir = self.resolve_path(workspace_dir)

        if workspace_dir in self._open_workspaces:
            raise ValidationError('Workspace already opened: %s' % workspace_dir)
        workspace_data_dir = Workspace.get_workspace_data_dir(workspace_dir)
        if os.path.isdir(workspace_data_dir):
            raise ValidationError('Workspace exists, consider opening it: %s' % workspace_dir)
        workspace = Workspace.create(workspace_dir, description=description)
        if is_scratch:
            workspace.is_scratch = True

        assert workspace_dir not in self._open_workspaces
        self._open_workspaces[workspace_dir] = workspace
        return workspace

    def open_workspace(self, workspace_dir: str, monitor: Monitor = Monitor.NONE) -> Workspace:
        workspace_dir = self.resolve_path(workspace_dir)
        workspace = self._open_workspaces.get(workspace_dir, None)
        if workspace is not None:
            assert not workspace.is_closed
            return workspace
        with monitor.starting("Opening workspace", 100):
            workspace = Workspace.open(workspace_dir, monitor=monitor.child(50))
            assert workspace_dir not in self._open_workspaces
            workspace.execute_workflow(monitor=monitor.child(50))
        self._open_workspaces[workspace_dir] = workspace
        return workspace

    def close_workspace(self, workspace_dir: str) -> None:
        workspace_dir = self.resolve_path(workspace_dir)
        workspace = self._open_workspaces.pop(workspace_dir, None)
        if workspace is not None:
            workspace.close()

    def close_all_workspaces(self) -> None:
        workspaces = self._open_workspaces.values()
        self._open_workspaces = dict()
        for workspace in workspaces:
            workspace.close()

    def save_workspace_as(self,
                          workspace_dir: str,
                          new_workspace_dir: str,
                          monitor: Monitor = Monitor.NONE) -> Workspace:
        workspace_dir = self.resolve_path(workspace_dir)
        workspace = self.get_workspace(workspace_dir)

        if '/' not in new_workspace_dir:
            new_workspace_dir = os.path.join(self.workspaces_dir, new_workspace_dir)
        else:
            new_workspace_dir = self.resolve_path(new_workspace_dir)

        empty_dir_exists = False
        if os.path.realpath(workspace_dir) == os.path.realpath(new_workspace_dir):
            return workspace

        with monitor.starting('opening "%s"' % new_workspace_dir, 100):
            if os.path.exists(new_workspace_dir):
                if os.path.isdir(new_workspace_dir):
                    entries = list(os.listdir(new_workspace_dir))
                    monitor.progress(work=5)
                    if len(entries) == 0:
                        empty_dir_exists = True
                    else:
                        raise ValidationError('Directory is not empty: %s' % new_workspace_dir)
                else:
                    raise ValidationError('A file with same name already exists: %s' % new_workspace_dir)
            else:
                monitor.progress(work=5)

            # Save and close current workspace
            workspace.save(monitor=monitor.child(work=25))
            workspace.close()

            # if the given directory exists and is empty, we must delete it because
            # shutil.copytree(workspace_dir, to_dir) expects to_dir to be non-existent
            if empty_dir_exists:
                os.rmdir(new_workspace_dir)
            monitor.progress(work=5)
            # Copy all files to new location to_dir
            shutil.copytree(workspace_dir, new_workspace_dir)
            monitor.progress(work=10)
            # Reopen from new location
            new_workspace = self.open_workspace(new_workspace_dir, monitor=monitor.child(work=50))
            # If it was a scratch workspace, delete the original
            if workspace.is_scratch:
                try:
                    shutil.rmtree(workspace_dir, ignore_errors=True)
                except (PermissionError, OSError):
                    pass
            monitor.progress(work=5)
            return new_workspace

    def save_workspace(self, workspace_dir: str, monitor: Monitor = Monitor.NONE) -> Workspace:
        workspace_dir = self.resolve_path(workspace_dir)
        workspace = self.get_workspace(workspace_dir)
        if workspace:
            workspace.save(monitor=monitor)
        return workspace

    def save_all_workspaces(self, monitor: Monitor = Monitor.NONE) -> None:
        workspaces = self._open_workspaces.values()
        n = len(workspaces)
        with monitor.starting('Saving %s workspace(s)' % n, n):
            for workspace in workspaces:
                workspace.save(monitor=monitor.child(work=1))

    def clean_workspace(self, workspace_dir: str) -> Workspace:
        workspace_dir = self.resolve_path(workspace_dir)
        workflow_file = Workspace.get_workflow_file(workspace_dir)
        old_workflow = None
        if os.path.isfile(workflow_file):
            # noinspection PyBroadException
            try:
                old_workflow = Workflow.load(workflow_file)
            except Exception:
                pass
            os.remove(workflow_file)
        old_workspace = self._open_workspaces.get(workspace_dir)
        if old_workspace:
            old_workspace.resource_cache.close()
        # Create new workflow but keep old header info
        workflow = Workspace.new_workflow(header=old_workflow.op_meta_info.header if old_workflow else None)
        workspace = Workspace(workspace_dir, workflow)
        self._open_workspaces[workspace_dir] = workspace
        workspace.save()
        return workspace

    def delete_workspace(self, workspace_dir: str, remove_completely: bool = False) -> None:
        workspace_dir = self.resolve_path(workspace_dir)
        self.close_workspace(workspace_dir)

        if remove_completely:
            shutil.rmtree(workspace_dir)
        else:
            workspace_data_dir = Workspace.get_workspace_data_dir(workspace_dir)
            if not os.path.isdir(workspace_data_dir):
                raise ValidationError('Not a workspace: %s' % workspace_dir)
            shutil.rmtree(workspace_data_dir)

    def run_op_in_workspace(self,
                            workspace_dir: str,
                            op_name: str,
                            op_args: OpKwArgs,
                            monitor: Monitor = Monitor.NONE) -> Union[Any, None]:
        workspace_dir = self.resolve_path(workspace_dir)
        workspace = self.get_workspace(workspace_dir)
        return workspace.run_op(op_name, op_args, monitor=monitor)

    def set_workspace_resource(self,
                               workspace_dir: str,
                               op_name: str,
                               op_args: OpKwArgs,
                               res_name: Optional[str] = None,
                               overwrite: bool = False,
                               monitor: Monitor = Monitor.NONE) -> Tuple[Workspace, str]:
        workspace_dir = self.resolve_path(workspace_dir)
        workspace = self.get_workspace(workspace_dir)
        res_name = workspace.set_resource(op_name, op_args, res_name, overwrite=overwrite, validate_args=True)
        workspace.execute_workflow(res_name=res_name, monitor=monitor)
        return workspace, res_name

    def rename_workspace_resource(self, workspace_dir: str,
                                  res_name: str, new_res_name: str) -> Workspace:
        workspace_dir = self.resolve_path(workspace_dir)
        workspace = self.get_workspace(workspace_dir)
        workspace.rename_resource(res_name, new_res_name)
        return workspace

    def delete_workspace_resource(self, workspace_dir: str, res_name: str) -> Workspace:
        workspace_dir = self.resolve_path(workspace_dir)
        workspace = self.get_workspace(workspace_dir)
        workspace.delete_resource(res_name)
        return workspace

    def set_workspace_resource_persistence(self,
                                           workspace_dir: str,
                                           res_name: str,
                                           persistent: bool) -> Workspace:
        workspace_dir = self.resolve_path(workspace_dir)
        workspace = self.get_workspace(workspace_dir)
        workspace.set_resource_persistence(res_name, persistent)
        return workspace

    def write_workspace_resource(self,
                                 workspace_dir: str,
                                 res_name: str,
                                 file_path: str,
                                 format_name: str = None,
                                 monitor: Monitor = Monitor.NONE) -> None:
        workspace_dir = self.resolve_path(workspace_dir)
        workspace = self.get_workspace(workspace_dir)
        target_path = self.resolve_path(file_path)
        with monitor.starting('Writing resource "%s"' % res_name, total_work=10):
            obj = workspace.execute_workflow(res_name=res_name, monitor=monitor.child(work=9))
            if obj is not None:
                write_object(obj, target_path, format_name=format_name)
                monitor.progress(work=1, msg='Writing file %s' % target_path)
            else:
                monitor.progress(work=1, msg='No output, file %s NOT written' % target_path)

    def plot_workspace_resource(self,
                                workspace_dir: str,
                                res_name: str,
                                var_name: str = None,
                                file_path: str = None,
                                monitor: Monitor = Monitor.NONE) -> None:
        workspace_dir = self.resolve_path(workspace_dir)
        workspace = self.get_workspace(workspace_dir)
        obj = self._get_resource_value(workspace, res_name, monitor)

        import xarray as xr
        import numpy as np
        # import matplotlib
        # matplotlib.use('Qt5Agg')
        import matplotlib.pyplot as plt

        if isinstance(obj, xr.Dataset):
            ds = obj
            if var_name:
                variables = [ds.data_vars[var_name]]
            else:
                variables = ds.data_vars.values()
            for var in variables:
                if hasattr(var, 'plot'):
                    _LOG.info('Plotting variable %s' % var)
                    var.plot()
            plt.show()
        elif isinstance(obj, xr.DataArray):
            var = obj
            if hasattr(var, 'plot'):
                _LOG.info('Plotting variable %s' % var)
                var.plot()
                plt.show()
        elif isinstance(obj, np.ndarray):
            plt.plot(obj)
            plt.show()
        else:
            raise ValidationError("Don't know how to plot an object of type \"%s\"" % type(obj))

    def print_workspace_resource(self,
                                 workspace_dir: str,
                                 res_name_or_expr: str = None,
                                 monitor: Monitor = Monitor.NONE) -> None:
        workspace_dir = self.resolve_path(workspace_dir)
        workspace = self.get_workspace(workspace_dir)
        value = self._get_resource_value(workspace, res_name_or_expr, monitor)
        pprint.pprint(value)

    # noinspection PyMethodMayBeStatic
    def _get_resource_value(self,
                            workspace: Workspace,
                            res_name_or_expr: str,
                            monitor: Monitor):
        value = UNDEFINED
        if res_name_or_expr is None:
            value = workspace.resource_cache
        elif res_name_or_expr.isidentifier() and workspace.workflow.find_node(res_name_or_expr) is not None:
            value = workspace.execute_workflow(res_name=res_name_or_expr, monitor=monitor)
        if value is UNDEFINED:
            value = safe_eval(res_name_or_expr, workspace.resource_cache)
        return value


def is_abs_windows_path(path: str) -> bool:
    """
    If normalized *path* an absolute path on Windows OS?
    :param path: normalized path
    :return: True, if so
    """
    return platform.system() == 'Windows' and is_abs_path(path)


def is_abs_path(path: str) -> bool:
    """
    If normalized *path* an absolute path on Windows OS?
    :param path: normalized path
    :return: True, if so
    """
    return path.startswith('//') or os.path.isabs(path)
