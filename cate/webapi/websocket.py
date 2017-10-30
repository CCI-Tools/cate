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

from collections import OrderedDict
from typing import List, Sequence, Optional

import xarray as xr

from cate.conf import conf
from cate.conf.defaults import VERSION_CONF_FILE
from cate.core.ds import DATA_STORE_REGISTRY
from cate.core.op import OP_REGISTRY
from cate.core.workspace import OpKwArgs
from cate.core.wsmanag import WorkspaceManager
from cate.util.monitor import Monitor
from cate.util.misc import cwd, filter_fileset

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

    def get_config(self) -> dict:
        return dict(data_stores_path=conf.get_data_stores_path(),
                    use_workspace_imagery_cache=conf.get_use_workspace_imagery_cache(),
                    default_res_pattern=conf.get_default_res_pattern())

    def set_config(self, config: dict) -> None:

        # Read existing config file as text
        conf_text = ''
        # noinspection PyBroadException
        try:
            with open(VERSION_CONF_FILE, 'r') as fp:
                conf_text = fp.read()
        except:
            # ok
            pass

        # Split into config file lines
        conf_lines = conf_text.split('\n')
        for key, value in config.items():
            new_entry = '%s = %s' % (key, repr(value))
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
        with open(VERSION_CONF_FILE, 'w') as fp:
            fp.write(conf_text)

    def get_data_stores(self) -> list:
        """
        Get registered data stores.

        :return: JSON-serializable list of data stores, sorted by name.
        """
        data_stores = sorted(DATA_STORE_REGISTRY.get_data_stores(), key=lambda ds: ds.title or ds.id)
        return [dict(id=data_store.id,
                     title=data_store.title,
                     isLocal=data_store.is_local) for data_store in data_stores]

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
        if data_store_id == 'esa_cci_odp':
            # Filter ESA Open Data Portal data sources
            data_source_dict = {ds.id: ds for ds in data_sources}
            # noinspection PyTypeChecker
            data_source_ids = filter_fileset(data_source_dict.keys(),
                                             includes=conf.get_config_value('included_data_sources', default=None),
                                             excludes=conf.get_config_value('excluded_data_sources', default=None))
            data_sources = [data_source_dict[ds_id] for ds_id in data_source_ids]

        data_sources = sorted(data_sources, key=lambda ds: ds.title or ds.id)
        return [dict(id=data_source.id,
                     title=data_source.title,
                     meta_info=data_source.meta_info) for data_source in data_sources]

    def get_data_source_temporal_coverage(self, data_store_id: str, data_source_id: str, monitor: Monitor) -> dict:
        """
        Get the temporal coverage of the data source.

        :param data_store_id: ID of the data store
        :param data_source_id: ID of the data source
        :param monitor: a progress monitor
        :return: JSON-serializable list of data sources, sorted by name.
        """
        data_store = DATA_STORE_REGISTRY.get_data_store(data_store_id)
        if data_store is None:
            raise ValueError('Unknown data store: "%s"' % data_store_id)
        data_sources = data_store.query(ds_id=data_source_id)
        if not data_sources:
            raise ValueError('data source "%s" not found' % data_source_id)
        data_source = data_sources[0]
        temporal_coverage = data_source.temporal_coverage(monitor=monitor)
        meta_info = OrderedDict()
        if temporal_coverage:
            start, end = temporal_coverage
            meta_info['temporal_coverage_start'] = start.strftime('%Y-%m-%d')
            meta_info['temporal_coverage_end'] = end.strftime('%Y-%m-%d')
        # TODO mz add available data information
        return meta_info

    def add_local_data_source(self, data_source_id: str, file_path_pattern: str, monitor: Monitor):
        """
        Adds a local data source made up of the specified files.

        :param data_source_id: The identifier of the local data source.
        :param file_path_pattern: The files path containing wildcards.
        :param monitor: a progress monitor.
        :return: JSON-serializable list of 'local' data sources, sorted by name.
        """
        data_store = DATA_STORE_REGISTRY.get_data_store('local')
        if data_store is None:
            raise ValueError('Unknown data store: "%s"' % 'local')
        with monitor.starting('Adding local data source', 100):
            # TODO use monitor, while extracting metadata
            data_store.add_pattern(data_source_id=data_source_id, files=file_path_pattern)
            return self.get_data_sources('local', monitor=monitor.child(100))

    def remove_local_data_source(self, data_source_id: str, remove_files: bool, monitor: Monitor) -> list:
        """
        Removes the datasource (and optionally the giles belonging  to it) from the local data store.

        :param data_source_id: The identifier of the local data source.
        :param remove_files: Wether to remove the files belonging to this data source.
        :param monitor: a progress monitor.
        :return: JSON-serializable list of 'local' data sources, sorted by name.
        """
        data_store = DATA_STORE_REGISTRY.get_data_store('local')
        if data_store is None:
            raise ValueError('Unknown data store: "%s"' % 'local')
        # TODO use monitor, while removing files
        data_store.remove_data_source(data_source_id, remove_files)
        return self.get_data_sources('local', monitor=monitor)

    def get_operations(self, registry=None) -> List[dict]:
        """
        Get registered operations.

        :return: JSON-serializable list of data sources, sorted by name.
        """
        registry = registry or OP_REGISTRY
        op_list = []
        for op_name, op_reg in registry.op_registrations.items():
            if op_reg.op_meta_info.header.get('deprecated'):
                continue
            op_json_dict = op_reg.op_meta_info.to_json_dict()
            op_json_dict['name'] = op_name
            op_json_dict['inputs'] = [dict(name=name, **props) for name, props in op_json_dict['inputs'].items()
                                      if not props.get('deprecated')]
            op_json_dict['outputs'] = [dict(name=name, **props) for name, props in op_json_dict['outputs'].items()
                                       if not props.get('deprecated')]
            op_list.append(op_json_dict)

        return sorted(op_list, key=lambda op: op['name'])

    def get_open_workspaces(self) -> Sequence[dict]:
        workspace_list = self.workspace_manager.get_open_workspaces()
        return [workspace.to_json_dict() for workspace in workspace_list]

    def get_workspace(self, base_dir: str) -> dict:
        workspace = self.workspace_manager.get_workspace(base_dir)
        return workspace.to_json_dict()

    # see cate-desktop: src/renderer.states.WorkspaceState
    def new_workspace(self, base_dir: str, description: str = None) -> dict:
        workspace = self.workspace_manager.new_workspace(base_dir, description)
        return workspace.to_json_dict()

    # see cate-desktop: src/renderer.states.WorkspaceState
    def open_workspace(self, base_dir: str, monitor: Monitor) -> dict:
        workspace = self.workspace_manager.open_workspace(base_dir, monitor=monitor)
        return workspace.to_json_dict()

    # see cate-desktop: src/renderer.states.WorkspaceState
    def close_workspace(self, base_dir: str) -> None:
        self.workspace_manager.close_workspace(base_dir)

    def close_all_workspaces(self) -> None:
        self.workspace_manager.close_all_workspaces()

    # see cate-desktop: src/renderer.states.WorkspaceState
    def save_workspace(self, base_dir: str, monitor: Monitor) -> dict:
        workspace = self.workspace_manager.save_workspace(base_dir, monitor=monitor)
        return workspace.to_json_dict()

    # see cate-desktop: src/renderer.states.WorkspaceState
    def save_workspace_as(self, base_dir: str, to_dir: str, monitor: Monitor) -> dict:
        workspace = self.workspace_manager.save_workspace_as(base_dir, to_dir, monitor=monitor)
        return workspace.to_json_dict()

    def save_all_workspaces(self, monitor: Monitor = Monitor.NONE) -> None:
        self.workspace_manager.save_all_workspaces(monitor=monitor)

    def clean_workspace(self, base_dir: str) -> dict:
        workspace = self.workspace_manager.clean_workspace(base_dir)
        return workspace.to_json_dict()

    def delete_workspace(self, base_dir: str) -> None:
        self.workspace_manager.delete_workspace(base_dir)

    def rename_workspace_resource(self, base_dir: str, res_name: str, new_res_name) -> dict:
        workspace = self.workspace_manager.rename_workspace_resource(base_dir, res_name, new_res_name)
        return workspace.to_json_dict()

    def delete_workspace_resource(self, base_dir: str, res_name: str) -> dict:
        workspace = self.workspace_manager.delete_workspace_resource(base_dir, res_name)
        return workspace.to_json_dict()

    def set_workspace_resource(self,
                               base_dir: str,
                               op_name: str,
                               op_args: OpKwArgs,
                               res_name: Optional[str],
                               overwrite: bool,
                               monitor: Monitor) -> list:
        with cwd(base_dir):
            workspace, res_name = self.workspace_manager.set_workspace_resource(base_dir,
                                                                                op_name,
                                                                                op_args,
                                                                                res_name=res_name,
                                                                                overwrite=overwrite,
                                                                                monitor=monitor)
            return [workspace.to_json_dict(), res_name]

    def set_workspace_resource_persistence(self, base_dir: str, res_name: str, persistent: bool) -> dict:
        with cwd(base_dir):
            workspace = self.workspace_manager.set_workspace_resource_persistence(base_dir, res_name, persistent)
            return workspace.to_json_dict()

    def write_workspace_resource(self, base_dir: str, res_name: str,
                                 file_path: str, format_name: str = None,
                                 monitor: Monitor = Monitor.NONE) -> None:
        with cwd(base_dir):
            self.workspace_manager.write_workspace_resource(base_dir, res_name, file_path,
                                                            format_name=format_name, monitor=monitor)

    def run_op_in_workspace(self, base_dir: str, op_name: str, op_args: OpKwArgs,
                            monitor: Monitor = Monitor.NONE) -> dict:
        with cwd(base_dir):
            workspace = self.workspace_manager.run_op_in_workspace(base_dir, op_name, op_args, monitor=monitor)
            return workspace.to_json_dict()

    def print_workspace_resource(self, base_dir: str, res_name_or_expr: str = None,
                                 monitor: Monitor = Monitor.NONE) -> None:
        with cwd(base_dir):
            self.workspace_manager.print_workspace_resource(base_dir,
                                                            res_name_or_expr=res_name_or_expr, monitor=monitor)

    def get_color_maps(self):
        from cate.util.im.cmaps import get_cmaps
        return get_cmaps()

    # Note, we should turn this into an operation "actual_min_max(ds, var)"
    def get_workspace_variable_statistics(self, base_dir: str, res_name: str, var_name: str, var_index: Sequence[int],
                                          monitor=Monitor.NONE):
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
                actual_min = variable.min(skipna=True)
            with monitor.child(work=50.).observing('Computing max'):
                actual_max = variable.max(skipna=True)

        return dict(min=float(actual_min), max=float(actual_max))
