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
import pprint
import shutil
import uuid
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from typing import List, Union, Optional, Tuple, Any

from .objectio import write_object
from .workflow import Workflow
from .workspace import Workspace, OpKwArgs
from ..conf.defaults import SCRATCH_WORKSPACES_PATH, WORKSPACE_DATA_DIR_NAME, WORKSPACES_DIR_NAME
from ..core.pathmanag import PathManager
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

    @abstractmethod
    def get_open_workspaces(self) -> List[Workspace]:
        pass

    @abstractmethod
    def get_workspace(self, base_dir: str) -> Workspace:
        pass

    @abstractmethod
    def list_workspace_names(self) -> List[str]:
        pass

    @abstractmethod
    def new_workspace(self, base_dir: Union[str, None], description: str = None) -> Workspace:
        pass

    @abstractmethod
    def open_workspace(self, base_dir: str,
                       monitor: Monitor = Monitor.NONE) -> Workspace:
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
    def save_workspace(self, base_dir: str,
                       monitor: Monitor = Monitor.NONE) -> Workspace:
        pass

    @abstractmethod
    def save_all_workspaces(self,
                            monitor: Monitor = Monitor.NONE) -> None:
        pass

    @abstractmethod
    def delete_workspace(self, base_dir: str, remove_completely: bool = False) -> None:
        pass

    @abstractmethod
    def clean_workspace(self, base_dir: str) -> None:
        pass

    @abstractmethod
    def run_op_in_workspace(self, base_dir: str,
                            op_name: str, op_args: OpKwArgs,
                            monitor: Monitor = Monitor.NONE) -> Union[Any, None]:
        pass

    @abstractmethod
    def set_workspace_resource(self,
                               base_dir: str,
                               op_name: str,
                               op_args: OpKwArgs,
                               res_name: Optional[str] = None,
                               overwrite: bool = False,
                               monitor: Monitor = Monitor.NONE) -> Tuple[Workspace, str]:
        pass

    @abstractmethod
    def rename_workspace_resource(self, base_dir: str,
                                  res_name: str, new_res_name: str) -> Workspace:
        pass

    @abstractmethod
    def delete_workspace_resource(self, base_dir: str, res_name: str) -> Workspace:
        pass

    @abstractmethod
    def set_workspace_resource_persistence(self, base_dir: str, res_name: str, persistent: bool) -> Workspace:
        pass

    @abstractmethod
    def write_workspace_resource(self, base_dir: str, res_name: str,
                                 file_path: str, format_name: str = None,
                                 monitor: Monitor = Monitor.NONE) -> None:
        pass

    @abstractmethod
    def plot_workspace_resource(self, base_dir: str, res_name: str,
                                var_name: str = None, file_path: str = None,
                                monitor: Monitor = Monitor.NONE) -> None:
        pass

    @abstractmethod
    def print_workspace_resource(self, base_dir: str, res_name_or_expr: str = None,
                                 monitor: Monitor = Monitor.NONE) -> None:
        pass

    @abstractmethod
    def _create_scratch_dir(self, scratch_dir_name: str) -> str:
        pass

    @abstractmethod
    def _resolve_target_path(self, target_dir: str) -> str:
        pass


class FSWorkspaceManager(WorkspaceManager):
    # TODO (forman, 20160908): implement file lock for opened workspaces (issue #26)
    def __init__(self, resolve_dir: str = None):
        self._open_workspaces = OrderedDict()
        self._resolve_dir = os.path.abspath(resolve_dir or os.curdir)

    def resolve_path(self, dir_path):
        if dir_path and os.path.isabs(dir_path):
            return os.path.normpath(dir_path)
        return os.path.abspath(os.path.join(self._resolve_dir, dir_path or ''))

    def num_open_workspaces(self) -> int:
        return len(self._open_workspaces)

    def get_open_workspaces(self) -> List[Workspace]:
        return list(self._open_workspaces.values())

    def get_workspace(self, base_dir: str) -> Workspace:
        base_dir = self.resolve_path(base_dir)
        workspace = self._open_workspaces.get(base_dir, None)
        if workspace is None:
            raise ValidationError('Workspace does not exist: %s' % base_dir)
        assert not workspace.is_closed
        # noinspection PyTypeChecker
        return workspace

    def list_workspace_names(self) -> List[str]:
        dir_list = []
        workspaces_dir = os.path.join(self._resolve_dir, WORKSPACES_DIR_NAME)
        search_dir = self.resolve_path(workspaces_dir)
        if not os.path.isdir(search_dir):
            return dir_list

        scan_list = os.scandir(search_dir)
        for entry in scan_list:
            if entry.is_dir():
                content_list = os.scandir(entry)
                for cont in content_list:
                    if cont.name == WORKSPACE_DATA_DIR_NAME:
                        dir_list.append(entry.name)

        return dir_list

    def new_workspace(self, base_dir: str, description: str = None) -> Workspace:
        is_scratch = False
        if base_dir is None:
            scratch_dir_name = str(uuid.uuid4())
            base_dir = self._create_scratch_dir(scratch_dir_name)
            is_scratch = True
        elif not os.path.isabs(base_dir):
            base_dir = os.path.join(WORKSPACES_DIR_NAME, base_dir)

        base_dir = self.resolve_path(base_dir)
        if base_dir in self._open_workspaces:
            raise ValidationError('Workspace already opened: %s' % base_dir)
        workspace_dir = Workspace.get_workspace_dir(base_dir)
        if os.path.isdir(workspace_dir):
            raise ValidationError('Workspace exists, consider opening it: %s' % base_dir)
        workspace = Workspace.create(base_dir, description=description)
        if is_scratch:
            workspace.is_scratch = True

        assert base_dir not in self._open_workspaces
        self._open_workspaces[base_dir] = workspace
        return workspace

    def open_workspace(self, base_dir: str, monitor: Monitor = Monitor.NONE) -> Workspace:
        if not os.path.isabs(base_dir):
            base_dir = os.path.join(WORKSPACES_DIR_NAME, base_dir)

        base_dir = self.resolve_path(base_dir)
        workspace = self._open_workspaces.get(base_dir, None)
        if workspace is not None:
            assert not workspace.is_closed
            # noinspection PyTypeChecker
            return workspace
        with monitor.starting("Opening workspace", 100):
            workspace = Workspace.open(base_dir, monitor=monitor.child(50))
            assert base_dir not in self._open_workspaces
            workspace.execute_workflow(monitor=monitor.child(50))
        self._open_workspaces[base_dir] = workspace
        return workspace

    def close_workspace(self, base_dir: str) -> None:
        if not os.path.isabs(base_dir):
            base_dir = os.path.join(WORKSPACES_DIR_NAME, base_dir)
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
        if not os.path.isabs(to_dir):
            to_dir = os.path.join(WORKSPACES_DIR_NAME, to_dir)

        to_dir = self._resolve_target_path(to_dir)

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
                        raise ValidationError('Directory is not empty: %s' % to_dir)
                else:
                    raise ValidationError('A file with same name already exists: %s' % to_dir)
            else:
                monitor.progress(work=5)

            # Save and close current workspace
            workspace.save(monitor=monitor.child(work=25))
            workspace.close()

            # if the given directory exists and is empty, we must delete it because
            # shutil.copytree(base_dir, to_dir) expects to_dir to be non-existent
            if empty_dir_exists:
                os.rmdir(to_dir)
            monitor.progress(work=5)
            # Copy all files to new location to_dir
            shutil.copytree(base_dir, to_dir)
            monitor.progress(work=10)
            # Reopen from new location
            new_workspace = self.open_workspace(to_dir, monitor=monitor.child(work=50))
            # If it was a scratch workspace, delete the original
            if workspace.is_scratch:
                try:
                    shutil.rmtree(base_dir, ignore_errors=True)
                except (PermissionError, OSError):
                    pass
            monitor.progress(work=5)
            return new_workspace

    def save_workspace(self, base_dir: str, monitor: Monitor = Monitor.NONE) -> Workspace:
        base_dir = self.resolve_path(base_dir)
        workspace = self.get_workspace(base_dir)
        if workspace:
            workspace.save(monitor=monitor)
        return workspace

    def save_all_workspaces(self, monitor: Monitor = Monitor.NONE) -> None:
        workspaces = self._open_workspaces.values()
        n = len(workspaces)
        with monitor.starting('Saving %s workspace(s)' % n, n):
            for workspace in workspaces:
                workspace.save(monitor=monitor.child(work=1))

    def clean_workspace(self, base_dir: str) -> Workspace:
        base_dir = self.resolve_path(base_dir)
        workflow_file = Workspace.get_workflow_file(base_dir)
        old_workflow = None
        if os.path.isfile(workflow_file):
            # noinspection PyBroadException
            try:
                old_workflow = Workflow.load(workflow_file)
            except Exception:
                pass
            os.remove(workflow_file)
        old_workspace = self._open_workspaces.get(base_dir)
        if old_workspace:
            old_workspace.resource_cache.close()
        # Create new workflow but keep old header info
        workflow = Workspace.new_workflow(header=old_workflow.op_meta_info.header if old_workflow else None)
        workspace = Workspace(base_dir, workflow)
        self._open_workspaces[base_dir] = workspace
        workspace.save()
        return workspace

    def delete_workspace(self, base_dir: str, remove_completely: bool = False) -> None:
        self.close_workspace(base_dir)
        if not os.path.isabs(base_dir):
            base_dir = os.path.join(WORKSPACES_DIR_NAME, base_dir)

        base_dir = self.resolve_path(base_dir)

        if remove_completely:
            shutil.rmtree(base_dir)
        else:
            workspace_dir = Workspace.get_workspace_dir(base_dir)
            if not os.path.isdir(workspace_dir):
                raise ValidationError('Not a workspace: %s' % base_dir)
            shutil.rmtree(workspace_dir)

    def run_op_in_workspace(self, base_dir: str,
                            op_name: str, op_args: OpKwArgs,
                            monitor: Monitor = Monitor.NONE) -> Union[Any, None]:
        workspace = self.get_workspace(base_dir)
        return workspace.run_op(op_name, op_args, monitor=monitor)

    def set_workspace_resource(self,
                               base_dir: str,
                               op_name: str,
                               op_args: OpKwArgs,
                               res_name: Optional[str] = None,
                               overwrite: bool = False,
                               monitor: Monitor = Monitor.NONE) -> Tuple[Workspace, str]:
        workspace = self.get_workspace(base_dir)
        res_name = workspace.set_resource(op_name, op_args, res_name, overwrite=overwrite, validate_args=True)
        workspace.execute_workflow(res_name=res_name, monitor=monitor)
        return workspace, res_name

    def rename_workspace_resource(self, base_dir: str,
                                  res_name: str, new_res_name: str) -> Workspace:
        workspace = self.get_workspace(base_dir)
        workspace.rename_resource(res_name, new_res_name)
        return workspace

    def delete_workspace_resource(self, base_dir: str, res_name: str) -> Workspace:
        workspace = self.get_workspace(base_dir)
        workspace.delete_resource(res_name)
        return workspace

    def set_workspace_resource_persistence(self, base_dir: str, res_name: str, persistent: bool) -> Workspace:
        workspace = self.get_workspace(base_dir)
        workspace.set_resource_persistence(res_name, persistent)
        return workspace

    def write_workspace_resource(self, base_dir: str, res_name: str,
                                 file_path: str, format_name: str = None,
                                 monitor: Monitor = Monitor.NONE) -> None:
        workspace = self.get_workspace(base_dir)
        target_path = self._resolve_target_path(file_path)
        with monitor.starting('Writing resource "%s"' % res_name, total_work=10):
            obj = workspace.execute_workflow(res_name=res_name, monitor=monitor.child(work=9))
            if obj is not None:
                write_object(obj, target_path, format_name=format_name)
                monitor.progress(work=1, msg='Writing file %s' % target_path)
            else:
                monitor.progress(work=1, msg='No output, file %s NOT written' % target_path)

    def plot_workspace_resource(self, base_dir: str, res_name: str,
                                var_name: str = None, file_path: str = None,
                                monitor: Monitor = Monitor.NONE) -> None:
        workspace = self.get_workspace(base_dir)
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

    def print_workspace_resource(self, base_dir: str, res_name_or_expr: str = None,
                                 monitor: Monitor = Monitor.NONE) -> None:
        workspace = self.get_workspace(base_dir)
        value = self._get_resource_value(workspace, res_name_or_expr, monitor)
        pprint.pprint(value)

    # noinspection PyMethodMayBeStatic
    def _get_resource_value(self, workspace, res_name_or_expr, monitor):
        value = UNDEFINED
        if res_name_or_expr is None:
            value = workspace.resource_cache
        elif res_name_or_expr.isidentifier() and workspace.workflow.find_node(res_name_or_expr) is not None:
            value = workspace.execute_workflow(res_name=res_name_or_expr, monitor=monitor)
        if value is UNDEFINED:
            value = safe_eval(res_name_or_expr, workspace.resource_cache)
        return value

    def _create_scratch_dir(self, scratch_dir_name: str) -> str:
        scratch_dir_path = os.path.join(SCRATCH_WORKSPACES_PATH, scratch_dir_name)
        os.makedirs(scratch_dir_path, exist_ok=True)
        return scratch_dir_path

    def _resolve_target_path(self, target_dir: str) -> str:
        return target_dir


class RelativeFSWorkspaceManager(FSWorkspaceManager):
    # TODO (forman, 20160908): implement file lock for opened workspaces (issue #26)

    def __init__(self, path_manager: PathManager):
        super().__init__(path_manager.get_root_path())
        self._path_manager = path_manager

    def resolve_path(self, dir_path):
        return self._path_manager.resolve(dir_path)

    def _create_scratch_dir(self, scratch_dir_name: str) -> str:
        scratch_dir_path = os.path.join(self._path_manager.get_scratch_dir_root(), scratch_dir_name)
        os.makedirs(scratch_dir_path, exist_ok=True)
        return scratch_dir_path

    def _resolve_target_path(self, target_dir: str) -> str:
        return self._path_manager.resolve(target_dir)
