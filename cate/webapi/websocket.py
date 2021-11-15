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

import datetime
import os
import platform
from typing import List, Sequence, Optional, Any, Tuple, Dict

import xarray as xr

from cate.conf import conf
from cate.conf.defaults import GLOBAL_CONF_FILE
from cate.conf.userprefs import set_user_prefs, get_user_prefs
from cate.core.ds import DATA_STORE_POOL
from cate.core.ds import add_as_local
from cate.core.ds import get_data_descriptor
from cate.core.ds import get_data_store_notices
from cate.core.ds import get_metadata_from_descriptor
from cate.core.op import OP_REGISTRY
from cate.core.workspace import OpKwArgs, Workspace
from cate.core.wsmanag import WorkspaceManager
from cate.util.misc import cwd
from cate.util.monitor import Monitor
from cate.util.sround import sround_range

__author__ = "Norman Fomferra (Brockmann Consult GmbH), " \
             "Marco Zühlke (Brockmann Consult GmbH)"


# noinspection PyMethodMayBeStatic
class WebSocketService:
    """
    Object which implements Cate's server-side methods.

    All methods receive inputs deserialized from JSON-RPC requests and must
    return JSON-serializable outputs.

    :param: workspace_manager The current workspace manager.
    """

    def __init__(self, workspace_manager: WorkspaceManager):
        self.workspace_manager = workspace_manager

    def _resolve_path(self, path: str) -> str:
        """Resolve incoming path against workspace manager's root path."""
        return self.workspace_manager.resolve_path(path)

    def _resolve_workspace_dir(self, workspace_dir_or_name: str) -> str:
        """Resolve incoming workspace dir path or name against workspace manager's root path."""
        # TODO (forman): remove me! this method exists, because we have
        #  workspace_manager.resolve_workspace_dir(), and this only because new_workspace()
        #  and save_workspace_as() take names instead of paths.
        return self.workspace_manager.resolve_workspace_dir(workspace_dir_or_name)

    def _serialize_workspace(self, workspace: Workspace) -> dict:
        """Serialize outgoing workspace JSON to have base_dir
        relative to workspace manager's root path."""
        workspace_json = workspace.to_json_dict()
        if self.workspace_manager.root_path:
            workspace_json['base_dir'] = \
                os.path.sep + os.path.relpath(workspace_json['base_dir'],
                                              self.workspace_manager.root_path)
        return workspace_json

    def keep_alive(self):
        """This operation is used to keep the WebSocket connection alive."""
        pass

    def get_config(self) -> dict:
        return dict(data_stores_path=conf.get_data_stores_path(),
                    use_workspace_imagery_cache=conf.get_use_workspace_imagery_cache(),
                    default_res_pattern=conf.get_default_res_pattern(),
                    http_proxy=conf.get_http_proxy())

    def set_config(self, config: dict) -> None:

        # Read existing config file as text
        conf_text = ''
        # noinspection PyBroadException
        try:
            with open(GLOBAL_CONF_FILE, 'r') as fp:
                conf_text = fp.read()
        except Exception:
            # ok
            pass

        # Split into config file lines
        conf_lines = conf_text.split('\n')
        for key, value in config.items():
            if value:
                new_entry = '%s = %s' % (key, repr(value))
            else:
                new_entry = '# %s =' % key
            # Try replacing existing code lines starting with key
            # Replace in reverse line order, because config files are interpreted top-down
            indices = list(range(len(conf_lines)))
            indices.reverse()
            inserted = False
            for i in indices:
                conf_line = conf_lines[i]
                if conf_line.startswith('#'):
                    # If comment, remove leading whitespaces
                    conf_line = conf_line[1:].lstrip()
                # If it starts with key,
                # next character must be '='
                # and line must not end with line continuation character
                if conf_line.startswith(key) \
                        and conf_line[len(key):].lstrip().index('=') >= 0 \
                        and not conf_line.endswith('\\'):
                    conf_lines[i] = new_entry
                    inserted = True
                    break
            if not inserted:
                conf_lines.append(new_entry)

        # Now join lines back again and write modified config file
        conf_text = '\n'.join(conf_lines)
        with open(GLOBAL_CONF_FILE, 'w') as fp:
            fp.write(conf_text)

    def get_data_stores(self) -> List[Dict[str, Any]]:
        """
        Get registered data stores.

        :return: JSON-serializable list of data stores, sorted by name.
        """
        # TODO add sensible notices
        data_stores = []
        for instance_id in sorted(DATA_STORE_POOL.store_instance_ids):
            config = DATA_STORE_POOL.get_store_config(instance_id)
            data_stores.append(dict(id=instance_id,
                                    title=config.title,
                                    isLocal=config.store_id == 'directory',
                                    description=config.description,
                                    notices=get_data_store_notices(instance_id)))
        return data_stores

    def get_data_sources(self, data_store_id: str, monitor: Monitor) -> List[Dict[str, Any]]:
        """
        Get data sources for a given data store.

        :param data_store_id: ID of the data store
        :param monitor: a progress monitor
        :return: JSON-serializable list of data sources, sorted by name.
        """
        data_store = DATA_STORE_POOL.get_store(data_store_id)
        if data_store is None:
            raise ValueError('Unknown data store: "%s"' % data_store_id)
        data_ids = list(data_store.get_data_ids(
            include_attrs=['title',
                           'verification_flags',
                           'data_type'])
        )
        data_sources = []
        with monitor.starting(f'Retrieving data sources for data store {data_store_id}',
                              total_work=len(data_ids)):
            for data_id, attrs in data_ids:
                data_sources.append(dict(
                    id=data_id,
                    title=attrs.get('title', data_id),
                    verification_flags=attrs.get('verification_flags'),
                    data_type=attrs.get('data_type'))
                )
                monitor.progress(1)
        return data_sources

    def get_data_source_meta_info(self,
                                  data_store_id: str,
                                  data_source_id: str,
                                  monitor: Monitor) -> Dict[str, Any]:
        """
        Get the meta data of the data source.

        :param data_store_id: ID of the data store
        :param data_source_id: ID of the data source
        :param monitor: a progress monitor
        :return: JSON-serializable list of data sources, sorted by name.
        """
        data_store = DATA_STORE_POOL.get_store(data_store_id)
        if data_store is None:
            raise ValueError('Unknown data store: "%s"' % data_store_id)
        with monitor.starting(f'Retrieving metadata for data source {data_source_id}'):
            data_source_descriptor = data_store.describe_data(data_source_id)
            return get_metadata_from_descriptor(data_source_descriptor)

    def add_local_data_source(self, data_source_id: str, file_path_pattern: str, monitor: Monitor):
        """
        Adds a local data source made up of the specified files.

        :param data_source_id: The identifier of the local data source.
        :param file_path_pattern: The files path containing wildcards.
        :param monitor: a progress monitor.
        :return: JSON-serializable list with the newly added local data source
        """
        data, data_id = add_as_local(data_source_id=data_source_id, paths=file_path_pattern)
        data_source = dict(id=data_id, title=data_id)
        descriptor = get_data_descriptor(data_id)
        if descriptor:
            data_source['data_type'] = descriptor.data_type
        return [data_source]

    def remove_local_data_source(self,
                                 data_source_id: str,
                                 remove_files: bool,
                                 monitor: Monitor) -> list:
        """
        Removes the datasource (and optionally the giles belonging  to it)
        from the local data store.

        :param data_source_id: The identifier of the local data source.
        :param remove_files: Wether to remove the files belonging to this data source.
        :param monitor: a progress monitor.
        :return: JSON-serializable list of 'local' data sources, sorted by name.
        """
        data_store = DATA_STORE_POOL.get_store('local')
        if data_store is None:
            raise ValueError('Unknown data store: "%s"' % 'local')
        # TODO use monitor, while removing files
        if remove_files:
            data_store.delete_data(data_source_id)
        else:
            data_store.deregister_data(data_source_id)
        return self.get_data_sources('local', monitor=monitor)

    def get_operations(self, registry=None) -> List[dict]:
        """
        Get registered operations.

        :return: JSON-serializable list of data sources, sorted by name.
        """
        registry = registry or OP_REGISTRY
        op_list = []
        for op_name, op_reg in registry.op_registrations.items():
            if op_reg.op_meta_info.header.get('deprecated') or op_name.startswith('_'):
                # do not list deprecated and private operations
                continue
            op_json_dict = op_reg.op_meta_info.to_json_dict()
            op_json_dict['name'] = op_name
            op_json_dict['inputs'] = [dict(name=name, **props)
                                      for name, props in op_json_dict['inputs'].items()
                                      if not (props.get('deprecated') or props.get('context'))]
            op_json_dict['outputs'] = [dict(name=name, **props)
                                       for name, props in op_json_dict['outputs'].items()
                                       if not props.get('deprecated')]
            op_list.append(op_json_dict)

        return sorted(op_list, key=lambda op: op['name'])

    def get_open_workspaces(self) -> Sequence[dict]:
        workspace_list = self.workspace_manager.get_open_workspaces()
        return [self._serialize_workspace(workspace) for workspace in workspace_list]

    def get_workspace(self, base_dir: str) -> dict:
        base_dir = self._resolve_workspace_dir(base_dir)
        workspace = self.workspace_manager.get_workspace(base_dir)
        return self._serialize_workspace(workspace)

    def list_workspace_names(self) -> Sequence[str]:
        workspace_names = self.workspace_manager.list_workspace_names()
        return workspace_names

    # see cate-desktop: src/renderer.states.WorkspaceState
    def new_workspace(self, base_dir: Optional[str], description: str = None) -> dict:
        base_dir = self._resolve_workspace_dir(base_dir) if base_dir else None
        workspace = self.workspace_manager.new_workspace(base_dir, description)
        return self._serialize_workspace(workspace)

    # see cate-desktop: src/renderer.states.WorkspaceState
    def open_workspace(self, base_dir: str, monitor: Monitor) -> dict:
        base_dir = self._resolve_workspace_dir(base_dir)
        with cwd(base_dir):
            workspace = self.workspace_manager.open_workspace(base_dir, monitor=monitor)
        return self._serialize_workspace(workspace)

    # see cate-desktop: src/renderer.states.WorkspaceState
    def close_workspace(self, base_dir: str) -> None:
        base_dir = self._resolve_workspace_dir(base_dir)
        self.workspace_manager.close_workspace(base_dir)

    def close_all_workspaces(self) -> None:
        self.workspace_manager.close_all_workspaces()

    def save_workspace(self, base_dir: str, monitor: Monitor) -> dict:
        base_dir = self._resolve_workspace_dir(base_dir)
        workspace = self.workspace_manager.save_workspace(base_dir, monitor=monitor)
        return self._serialize_workspace(workspace)

    def save_workspace_as(self, base_dir: str, to_dir: str, monitor: Monitor) -> dict:
        base_dir = self._resolve_workspace_dir(base_dir)
        workspace = self.workspace_manager.save_workspace_as(base_dir, to_dir, monitor=monitor)
        return self._serialize_workspace(workspace)

    def save_all_workspaces(self, monitor: Monitor = Monitor.NONE) -> None:
        self.workspace_manager.save_all_workspaces(monitor=monitor)

    def clean_workspace(self, base_dir: str) -> dict:
        base_dir = self._resolve_workspace_dir(base_dir)
        workspace = self.workspace_manager.clean_workspace(base_dir)
        return self._serialize_workspace(workspace)

    def delete_workspace(self, base_dir: str, remove_completely: bool = False) -> None:
        base_dir = self._resolve_workspace_dir(base_dir)
        self.workspace_manager.delete_workspace(base_dir, remove_completely)

    def rename_workspace_resource(self, base_dir: str, res_name: str, new_res_name) -> dict:
        base_dir = self._resolve_workspace_dir(base_dir)
        workspace = self.workspace_manager.rename_workspace_resource(base_dir,
                                                                     res_name,
                                                                     new_res_name)
        return self._serialize_workspace(workspace)

    def delete_workspace_resource(self, base_dir: str, res_name: str) -> dict:
        base_dir = self._resolve_workspace_dir(base_dir)
        workspace = self.workspace_manager.delete_workspace_resource(base_dir, res_name)
        return self._serialize_workspace(workspace)

    def set_workspace_resource(self,
                               base_dir: str,
                               op_name: str,
                               op_args: OpKwArgs,
                               res_name: Optional[str],
                               overwrite: bool,
                               monitor: Monitor) -> list:
        base_dir = self._resolve_workspace_dir(base_dir)
        with cwd(base_dir):
            workspace, res_name = \
                self.workspace_manager.set_workspace_resource(base_dir,
                                                              op_name,
                                                              op_args,
                                                              res_name=res_name,
                                                              overwrite=overwrite,
                                                              monitor=monitor)
            return [self._serialize_workspace(workspace), res_name]

    def set_workspace_resource_persistence(self,
                                           base_dir: str,
                                           res_name: str,
                                           persistent: bool) -> dict:
        base_dir = self._resolve_workspace_dir(base_dir)
        with cwd(base_dir):
            workspace = self.workspace_manager.set_workspace_resource_persistence(base_dir,
                                                                                  res_name,
                                                                                  persistent)
            return self._serialize_workspace(workspace)

    def write_workspace_resource(self, base_dir: str, res_name: str,
                                 file_path: str, format_name: str = None,
                                 monitor: Monitor = Monitor.NONE) -> None:
        base_dir = self._resolve_workspace_dir(base_dir)
        with cwd(base_dir):
            self.workspace_manager.write_workspace_resource(base_dir,
                                                            res_name,
                                                            file_path,
                                                            format_name=format_name,
                                                            monitor=monitor)

    def run_op_in_workspace(self, base_dir: str, op_name: str, op_args: OpKwArgs,
                            monitor: Monitor = Monitor.NONE) -> Optional[Any]:
        with cwd(base_dir):
            return self.workspace_manager.run_op_in_workspace(base_dir,
                                                              op_name,
                                                              op_args,
                                                              monitor=monitor)

    def extract_pixel_values(self, base_dir: str, source: str,
                             point: Tuple[float, float], indexers: dict) -> dict:
        base_dir = self._resolve_workspace_dir(base_dir)
        with cwd(base_dir):
            from cate.ops.subset import extract_point
            ds = self.workspace_manager.get_workspace(base_dir).resource_cache.get(source)
            if ds is None:
                return {}
            return extract_point(ds, point, indexers)

    def print_workspace_resource(self, base_dir: str, res_name_or_expr: str = None,
                                 monitor: Monitor = Monitor.NONE) -> None:
        base_dir = self._resolve_workspace_dir(base_dir)
        with cwd(base_dir):
            self.workspace_manager.print_workspace_resource(base_dir,
                                                            res_name_or_expr=res_name_or_expr,
                                                            monitor=monitor)

    def get_color_maps(self):
        from cate.util.im.cmaps import get_cmaps
        return get_cmaps()

    # Note, we should turn this into an operation "actual_min_max(ds, var)"
    def get_workspace_variable_statistics(self,
                                          base_dir: str,
                                          res_name: str,
                                          var_name: str,
                                          var_index: Sequence[int],
                                          monitor=Monitor.NONE):
        base_dir = self._resolve_workspace_dir(base_dir)
        workspace_manager = self.workspace_manager
        workspace = workspace_manager.get_workspace(base_dir)
        if res_name not in workspace.resource_cache:
            raise ValueError('Unknown resource "%s"' % res_name)

        dataset = workspace.resource_cache[res_name]
        if not isinstance(dataset, xr.Dataset):
            raise ValueError('Resource "%s" must be a Dataset' % res_name)

        if var_name not in dataset:
            raise ValueError('Variable "%s" not found in "%s"' % (var_name, res_name))

        variable = dataset[var_name]
        if var_index:
            variable = variable[tuple(var_index)]

        with monitor.starting('Computing min/max', total_work=100.):
            with monitor.child(work=50.).observing('Computing min'):
                actual_min = float(variable.min(skipna=True))
            with monitor.child(work=50.).observing('Computing max'):
                actual_max = float(variable.max(skipna=True))

        actual_min, actual_max = sround_range((actual_min, actual_max), ndigits=2)
        return dict(min=actual_min, max=actual_max)

    def set_preferences(self, prefs: dict):
        set_user_prefs(prefs)

    def get_preferences(self) -> dict:
        return get_user_prefs()

    def update_file_node(self, path: str) -> dict:
        """
        Return updated file node at *path*.
        :param path: A normalized, absolute path that never has a trailing "/".
        :return: A JSON dictionary containing an updated file node at *path*.
        """
        if self.workspace_manager.root_path:
            path = self._resolve_path(path)

        if platform.system() == 'Windows':
            drive_letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            if not path:
                child_nodes = []
                for letter in drive_letters:
                    drive_path = f'{letter}:/'
                    if os.path.exists(drive_path):
                        child_node = _new_file_node(f'{letter}:', is_dir=True)
                        try:
                            stat_result = os.stat(drive_path)
                            _update_file_node_stat_result(child_node, stat_result)
                        except OSError as error:
                            _update_file_node_error(child_node, error)
                        child_nodes.append(child_node)
                return _new_file_node('', is_dir=True, child_nodes=child_nodes, status="ready")
            if len(path) == 2 and path[0] in drive_letters and path[1] == ':':
                basename = path
                path = basename + '/'
            else:
                basename = os.path.basename(path)
        elif path == '':
            path = '/'
            basename = ''
        elif path == '/':
            basename = ''
        else:
            basename = os.path.basename(path)

        if path == self.workspace_manager.root_path or basename == '.':
            basename = ''

        if basename.startswith('.'):
            # Make it a little securer
            raise ValueError('cannot update hidden files')

        child_nodes = None
        is_dir = os.path.isdir(path)
        if is_dir:
            child_nodes = []
            dir_it = None
            try:
                dir_it = os.scandir(path)
                for dir_entry in dir_it:
                    name = str(dir_entry.name)
                    if name.startswith('.'):
                        continue
                    child_node = _new_file_node(name,
                                                is_dir=dir_entry.is_dir(),
                                                status=None)
                    try:
                        stat_result = dir_entry.stat()
                        _update_file_node_stat_result(child_node, stat_result=stat_result)
                    except OSError as error:
                        _update_file_node_error(child_node, error)
                    child_nodes.append(child_node)
            except OSError as error:
                file_node = _new_file_node(basename, is_dir=is_dir)
                return _update_file_node_error(file_node, error)
            finally:
                if dir_it is not None:
                    # noinspection PyUnresolvedReferences
                    dir_it.close()
        file_node = _new_file_node(basename,
                                   is_dir=is_dir,
                                   child_nodes=child_nodes,
                                   status='ready')
        try:
            stat_result = os.stat(path)
            return _update_file_node_stat_result(file_node, stat_result)
        except OSError as error:
            return _update_file_node_error(file_node, error)


def _new_file_node(name: str,
                   is_dir: bool = False,
                   child_nodes: List[Dict] = None,
                   status=None) -> Dict:
    file_node = {
        "name": name,
        "isDir": is_dir
    }
    if child_nodes is not None:
        file_node["childNodes"] = child_nodes
    if status is not None:
        file_node["status"] = status
    return file_node


def _update_file_node_stat_result(file_node: Dict, stat_result) -> Dict:
    file_node["lastModified"] = _format_time(stat_result.st_mtime)
    file_node["size"] = stat_result.st_size
    return file_node


def _update_file_node_error(file_node: Dict, error) -> Dict:
    file_node["status"] = "error"
    file_node["message"] = f'{error}'
    return file_node


def _format_time(seconds):
    text = datetime.datetime.fromtimestamp(seconds).isoformat()
    if '.' in text:
        text = text[0:text.rindex('.')]
    if 'T' in text:
        text = text.replace('T', ' ')
    return text
