# The MIT License (MIT)
# Copyright (c) 2016-2023 by the ESA CCI Toolbox team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

from abc import ABCMeta, abstractmethod
from typing import List, Optional, Tuple, Any, Union

from cate.core.workspace import OpKwArgs
from cate.core.workspace import Workspace
from cate.util.monitor import Monitor


class WorkspaceManager(metaclass=ABCMeta):
    """
    Abstract base class which represents the ``WorkspaceManager`` interface.
    """

    @property
    @abstractmethod
    def root_path(self) -> Optional[str]:
        pass

    @abstractmethod
    def resolve_path(self, id_or_path: Union[int, str]) -> str:
        pass

    # TODO (forman): remove me! this method exists,
    #  because new_workspace() and save_workspace_as() take names
    #  instead of paths. Better to add flags to new_workspace()
    #  and save_workspace_as() to indicate
    #  they are not paths, but just names which will be
    #  relative to default "~/workspaces" location.
    @abstractmethod
    def resolve_workspace_dir(self, path_or_name: str) -> str:
        pass

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
    def run_op_in_workspace(
            self,
            workspace_dir: str,
            op_name: str,
            op_args: OpKwArgs,
            monitor: Monitor = Monitor.NONE
    ) -> Optional[Any]:
        pass

    @abstractmethod
    def set_workspace_resource(
            self,
            workspace_dir: str,
            op_name: str,
            op_args: OpKwArgs,
            res_name: Optional[str] = None,
            overwrite: bool = False,
            monitor: Monitor = Monitor.NONE
    ) -> Tuple[Workspace, str]:
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
