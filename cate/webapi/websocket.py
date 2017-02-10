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

__author__ = "Norman Fomferra (Brockmann Consult GmbH), " \
             "Marco Zühlke (Brockmann Consult GmbH)"

from collections import OrderedDict
from typing import List, Sequence

import xarray as xr

from cate.conf import conf
from cate.conf.defaults import DEFAULT_CONF_FILE, WEBAPI_USE_WORKSPACE_IMAGERY_CACHE
from cate.core.ds import DATA_STORE_REGISTRY, get_data_stores_path
from cate.core.op import OP_REGISTRY
from cate.core.wsmanag import WorkspaceManager
from cate.util import Monitor, cwd, to_str_constant, is_str_constant


# noinspection PyMethodMayBeStatic
class WebSocketService:
    """
    Object which implements Cate's server-side methods.

    All methods receive inputs deserialized from JSON-RCP requests and must
    return JSON-serializable outputs.

    :param: workspace_manager The current workspace manager.
    """

    def __init__(self, workspace_manager: WorkspaceManager):
        self.workspace_manager = workspace_manager

    def get_config(self) -> dict:
        config = conf.get_config()
        return dict(data_stores_path=get_data_stores_path(),
                    use_workspace_imagery_cache=config.get('use_workspace_imagery_cache',
                                                           WEBAPI_USE_WORKSPACE_IMAGERY_CACHE))

    def set_config(self, config: dict) -> None:

        # Read existing config file as text
        # noinspection PyBroadException
        conf_text = ''
        try:
            with open(DEFAULT_CONF_FILE, 'r') as fp:
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
        with open(DEFAULT_CONF_FILE, 'w') as fp:
            fp.write(conf_text)

    def get_data_stores(self) -> list:
        """
        Get registered data stores.

        :return: JSON-serializable list of data stores, sorted by name.
        """
        data_stores = DATA_STORE_REGISTRY.get_data_stores()
        data_store_list = []
        for data_store in data_stores:
            data_store_list.append(dict(id=data_store.name,
                                        name=data_store.name,
                                        description=''))

        return sorted(data_store_list, key=lambda ds: ds['name'])

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
        data_source_list = []
        for data_source in data_sources:
            data_source_list.append(dict(id=data_source.name,
                                         name=data_source.name,
                                         meta_info=data_source.meta_info))

        return sorted(data_source_list, key=lambda ds: ds['name'])

    def get_ds_temporal_coverage(self, data_store_id: str, data_source_id: str, monitor: Monitor) -> dict:
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
        data_sources = data_store.query(name=data_source_id)
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

    def get_operations(self) -> List[dict]:
        """
        Get registered operations.

        :return: JSON-serializable list of data sources, sorted by name.
        """
        op_list = []
        for op_name, op_reg in OP_REGISTRY.op_registrations.items():
            op_json_dict = op_reg.op_meta_info.to_json_dict()
            op_json_dict['name'] = op_name
            op_json_dict['input'] = [dict(name=name, **props) for name, props in op_json_dict['input'].items()]
            op_json_dict['output'] = [dict(name=name, **props) for name, props in op_json_dict['output'].items()]
            op_list.append(op_json_dict)

        return sorted(op_list, key=lambda op: op['name'])

    # see cate-desktop: src/renderer.states.WorkspaceState
    def new_workspace(self, base_dir: str) -> dict:
        workspace = self.workspace_manager.new_workspace(base_dir)
        return workspace.to_json_dict()

    # see cate-desktop: src/renderer.states.WorkspaceState
    def open_workspace(self, base_dir: str, monitor: Monitor) -> dict:
        workspace = self.workspace_manager.open_workspace(base_dir, monitor=monitor)
        return workspace.to_json_dict()

    # see cate-desktop: src/renderer.states.WorkspaceState
    def close_workspace(self, base_dir: str) -> None:
        self.workspace_manager.close_workspace(base_dir)

    # see cate-desktop: src/renderer.states.WorkspaceState
    def save_workspace(self, base_dir: str) -> dict:
        workspace = self.workspace_manager.save_workspace(base_dir)
        return workspace.to_json_dict()

    # see cate-desktop: src/renderer.states.WorkspaceState
    def save_workspace_as(self, base_dir: str, to_dir: str, monitor: Monitor) -> dict:
        workspace = self.workspace_manager.save_workspace_as(base_dir, to_dir, monitor=monitor)
        return workspace.to_json_dict()

    # noinspection PyAbstractClass
    def set_workspace_resource(self, base_dir: str, res_name: str, op_name: str, op_args: dict,
                               monitor: Monitor) -> dict:
        # TODO (nf): op_args come in as {"name1": {value: value1}, "name2": {source: value2}, ...}
        # Due to the current CLI and REST API implementation we must encode this coding to distinguish
        # constant values from workflow step IDs (= resource names).
        # If this called from cate-desktop, op_args could already be a proper typed + validated JSON dict
        encoded_op_args = []
        for name, value_obj in op_args.items():
            if 'value' in value_obj:
                value = value_obj['value']
                if isinstance(value, str) and not is_str_constant(value):
                    value = to_str_constant(value)
                encoded_op_arg = '%s=%s' % (name, value)
            elif 'source' in value_obj:
                source = value_obj['source']
                encoded_op_arg = '%s=%s' % (name, source)
            else:
                raise ValueError('illegal operation argument: %s=%s' % (name, value_obj))
            encoded_op_args.append(encoded_op_arg)
        with cwd(base_dir):
            workspace = self.workspace_manager.set_workspace_resource(base_dir,
                                                                      res_name,
                                                                      op_name,
                                                                      op_args=encoded_op_args,
                                                                      monitor=monitor)
            return workspace.to_json_dict()

    # noinspection PyMethodMayBeStatic
    def get_color_maps(self):
        from cate.util.im.cmaps import get_cmaps
        return get_cmaps()

    def get_workspace_variable_statistics(self, base_dir: str, res_name: str, var_name: str, var_index: Sequence[int]):
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
            variable = variable[var_index]

        valid_min = variable.min(skipna=True)
        valid_max = variable.max(skipna=True)

        return dict(min=float(valid_min), max=float(valid_max))
