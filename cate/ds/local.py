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
__author__ = "Norman Fomferra (Brockmann Consult GmbH), " \
             "Marco Zühlke (Brockmann Consult GmbH), " \
             "Chris Bernat (Telespazio VEGA UK Ltd)"

"""
Description
===========

This plugin module adds the local data source to the data store registry.

Verification
============

The module's unit-tests are located in
`test/ds/test_esa_cci_ftp.py <https://github.com/CCI-Tools/cate-core/blob/master/test/ds/test_esa_cci_ftp.py>`_
and may be executed using ``$ py.test test/ds/test_esa_cci_ftp.py --cov=cate/ds/esa_cci_ftp.py``
for extra code coverage information.

Components
==========
"""

import json
import os
import shutil
import xarray as xr
from collections import OrderedDict
from datetime import datetime
from dateutil import parser
from glob import glob
from typing import Optional, Sequence, Union, Any, Tuple
from xarray.backends.netCDF4_ import NetCDF4DataStore

from cate.conf import get_config_value
from cate.conf.defaults import NETCDF_COMPRESSION_LEVEL
from cate.core.ds import DATA_STORE_REGISTRY, DataStore, DataSource, open_xarray_dataset
from cate.core.ds import get_data_stores_path
from cate.core.types import GeometryLike, TimeRange, TimeRangeLike, VariableNamesLike
from cate.util.misc import to_list
from cate.util.monitor import Monitor
from cate.ds.config import LocalDataSourceConfiguration
_REFERENCE_DATA_SOURCE_TYPE = "FILE_PATTERN"


def get_data_store_path():
    return os.environ.get('CATE_LOCAL_DATA_STORE_PATH',
                          os.path.join(get_data_stores_path(), 'local'))


def add_to_data_store_registry():
    data_store = LocalDataStore('local', get_data_store_path())
    DATA_STORE_REGISTRY.add_data_store(data_store)


class LocalDataSource(DataSource):
    def __init__(self, name: str, files: Union[Sequence[str], OrderedDict], data_store: DataStore,
                 config: LocalDataSourceConfiguration = None, reference_type: str = None, reference_name: str = None):
        self._name = name
        if isinstance(files, Sequence):
            self._files = OrderedDict.fromkeys(files)
        else:
            self._files = files
        self._data_store = data_store
        initial_temporal_coverage = None
        files_number = len(self._files.items())
        if files_number > 0:
            files_range = list(self._files.values())
            if files_range:
                if isinstance(files_range[0], Tuple):
                    initial_temporal_coverage = TimeRangeLike.convert(tuple([files_range[0][0],
                                                                             files_range[files_number-1][1]]))
                elif isinstance(files_range[0], datetime):
                    initial_temporal_coverage = TimeRangeLike.convert((files_range[0],
                                                                       files_range[files_number-1]))
        self._config = config if config else \
            LocalDataSourceConfiguration(name=name, data_store=data_store,
                                         config_type=reference_type if reference_type else _REFERENCE_DATA_SOURCE_TYPE,
                                         source=reference_name, temporal_coverage=initial_temporal_coverage)

    def open_dataset(self,
                     time_range: TimeRangeLike.TYPE = None,
                     region: GeometryLike.TYPE = None,
                     var_names: VariableNamesLike.TYPE = None,
                     protocol: str = None) -> Any:
        time_range = TimeRangeLike.convert(time_range) if time_range else None
        if region:
            region = GeometryLike.convert(region)
        if var_names:
            var_names = VariableNamesLike.convert(var_names)
        paths = []
        if time_range:
            time_series = list(self._files.values())
            file_paths = list(self._files.keys())
            for i in range(len(time_series)):
                if time_series[i]:
                    if isinstance(time_series[i], Tuple) and \
                            time_series[i][0] >= time_range[0] and \
                            time_series[i][1] <= time_range[1]:
                        paths.extend(glob(os.path.join(get_data_store_path(), file_paths[i])))
                    elif isinstance(time_series[i], datetime) and \
                            time_range[0] <= time_series[i] < time_range[1]:
                        paths.extend(glob(os.path.join(get_data_store_path(), file_paths[i])))
        else:
            for file in self._files.items():
                paths.extend(glob(os.path.join(self._data_store.data_store_path, file[0])))
            paths = sorted(set(paths))
        if paths:
            try:
                ds = open_xarray_dataset(paths)
                if region:
                    [lat_min, lon_min, lat_max, lon_max] = region.bounds
                    ds = ds.sel(drop=False, lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max))
                if var_names:
                    ds = ds.drop([var_name for var_name in ds.variables.keys() if var_name not in var_names])
                return ds
            except OSError as e:
                raise IOError("Files: {} caused:\nOSError({}): {}".format(paths, e.errno, e.strerror))
        else:
            return None

    def _make_local(self,
                    local_name: str,
                    local_id: str = None,
                    time_range: TimeRangeLike.TYPE = None,
                    region: GeometryLike.TYPE = None,
                    var_names: VariableNamesLike.TYPE = None,
                    monitor: Monitor = Monitor.NONE) -> str:
        if not local_id:
            local_id = local_name
        time_range = TimeRangeLike.convert(time_range) if time_range else None
        region = GeometryLike.convert(region) if region else None
        var_names = VariableNamesLike.convert(var_names) if var_names else None

        local_path = os.path.join(get_data_store_path(), local_id)

        config = LocalDataSourceConfiguration(name=local_id, data_store=self._data_store,
                                              config_type=_REFERENCE_DATA_SOURCE_TYPE, source=self._name)

        compression_level = get_config_value('NETCDF_COMPRESSION_LEVEL', NETCDF_COMPRESSION_LEVEL)
        compression_enabled = True if compression_level > 0 else False

        encoding_update = dict()
        if compression_enabled:
            encoding_update.update({'zlib': True, 'complevel': compression_level})

        remote_paths = [glob(file[0] for file in self._files.items())]
        if not remote_paths:
            raise ValueError("Cannot make local copy of empty DataStore")
        monitor.start("make local", total_work=len(remote_paths))
        for remote_path in sorted(set(remote_paths)):
            child_monitor = monitor.child(work=1)

            remote_netcdf = NetCDF4DataStore(remote_path)
            start_date = parser.parse(remote_netcdf.attrs['start_date'])
            stop_date = parser.parse(remote_netcdf.attrs['stop_date'])
            if start_date >= time_range[0] and stop_date <= time_range[1]:

                remote_dataset = xr.Dataset.load_store(remote_netcdf)

                local_netcdf = None

                file_name = os.path.basename(remote_path)

                if region or var_names:
                    try:
                        local_filepath = os.path.join(local_path, file_name)

                        local_netcdf = NetCDF4DataStore(local_filepath, mode='w', persist=True)
                        local_netcdf.set_attributes(remote_netcdf.get_attrs())

                        if region:
                            [lat_min, lon_min, lat_max, lon_max] = region.bounds
                            remote_dataset = remote_dataset.sel(drop=False,
                                                                lat=slice(lat_min, lat_max),
                                                                lon=slice(lon_min, lon_max))
                        if not var_names:
                            var_names = [var_name for var_name in remote_netcdf.variables.keys()]
                        var_names.extend([coord_name for coord_name in remote_dataset.coords.keys()
                                          if coord_name not in var_names])

                        child_monitor.start(label=file_name, total_work=len(var_names))
                        for sel_var_name in var_names:
                            var_dataset = remote_dataset.drop(
                                [var_name for var_name in var_names if var_name != sel_var_name])
                            local_netcdf.store_dataset(var_dataset)
                            child_monitor.progress(work=1, msg=sel_var_name)
                    finally:
                        if remote_netcdf:
                            remote_netcdf.close()
                        if local_netcdf:
                            local_netcdf.close()
                else:
                    local_filepath = shutil.copy(src=remote_path, dst=local_path)
                config.add_file(local_filepath, (start_date, stop_date))
            child_monitor.done()
        config.save()
        monitor.done()

        return local_id

    def make_local(self,
                   local_name: str,
                   local_id: str = None,
                   time_range: TimeRangeLike.TYPE = None,
                   region: GeometryLike.TYPE = None,
                   var_names: VariableNamesLike.TYPE = None,
                   monitor: Monitor = Monitor.NONE) -> str:
        if not local_name:
            raise ValueError('local_name is required')
        elif len(local_name) == 0:
            raise ValueError('local_name cannot be empty')
        local_path = os.path.join(get_data_store_path(), local_name)
        try:
            os.makedirs(local_path, exist_ok=False)
        except FileExistsError:
            raise ValueError("Local data source with such name already exists", local_name)
        return self._make_local(local_name, local_id, time_range, region, var_names, monitor)

    def add_dataset(self, file, time_coverage: TimeRangeLike.TYPE = None, update: bool = False):
        if update or self._files.keys().isdisjoint([file]):
            self._files[file] = time_coverage
            self._config.add_file(file, time_coverage)
        self._files = OrderedDict(sorted(self._files.items(), key=lambda f: f[1] if isinstance(f, Tuple) and f[1]
                                                                            else datetime.max))

    def remove_time_range(self, time_coverage: TimeRangeLike.TYPE):
        files_to_remove = []
        for file, time_range in self._files.items():
            if time_coverage[0] <= time_range[0] <= time_coverage[1] \
                    and time_coverage[0] <= time_range[1] <= time_coverage[1]:
                files_to_remove.append(file)
                self._config.remove_file(file)
        for file in files_to_remove:
            del self._files[file]

    def save(self):
        self._config.save()

    def temporal_coverage(self, monitor: Monitor = Monitor.NONE) -> Optional[TimeRange]:
        return self._config.temporal_coverage

    def spatial_coverage(self):
        return self._config.region

    @property
    def variables_info(self):
        return self._config.var_names

    @property
    def info_string(self):
        return 'Files: %s' % (' '.join(self._files))

    def _repr_html_(self):
        import html
        return '<table style="border:0;">\n' \
               '<tr><td>Name</td><td><strong>%s</strong></td></tr>\n' \
               '<tr><td>Files</td><td><strong>%s</strong></td></tr>\n' \
               '</table>\n' % (html.escape(self._name), html.escape(' '.join(self._files)))

    @property
    def data_store(self) -> DataStore:
        return self._data_store

    @property
    def name(self) -> str:
        return self._name

    def to_json_dict(self):
        """
        Return a JSON-serializable dictionary representation of this object.

        :return: A JSON-serializable dictionary
        """
        fsds_dict = OrderedDict()
        fsds_dict['name'] = self.name
        fsds_dict['files'] = list(self._files.items())
        return fsds_dict

    @classmethod
    def from_json_dict(cls, json_dicts: dict, data_store: DataStore) -> Optional['LocalDataSource']:

        name = json_dicts.get('name')
        files = json_dicts.get('files', None)
        if name and isinstance(files, list):
            if len(files) > 0:
                if isinstance(files[0], list):
                    file_details_length = len(files[0])
                    if file_details_length > 2:
                        files = OrderedDict((item[0], (parser.parse(item[1]).replace(microsecond=0),
                                             parser.parse(item[2]).replace(microsecond=0))
                                             if item[1] and item[2] else None) for item in files)
                    else:
                        files = OrderedDict((item[0], parser.parse(item[1]).replace(microsecond=0))
                                            if len(item) > 1 else (item[0], None) for item in files)
            else:
                files = OrderedDict()
            return LocalDataSource(name, files, data_store)
        return None


class LocalDataStore(DataStore):
    def __init__(self, name: str, store_dir: str):
        super().__init__(name)
        self._store_dir = store_dir
        self._data_sources = None

    def add_pattern(self, name: str, files: Union[str, Sequence[str]] = None) -> 'DataSource':
        data_source = self.create_data_source(name)
        if isinstance(files, str):
            files = [files]
        for file in files:
            data_source.add_dataset(file)
        return data_source

    def create_data_source(self, name: str, reference_type: str = None, reference_name: str = None):
        self._init_data_sources()
        if not name.startswith('%s.' % self.name):
            name = '%s.%s' % (self.name, name)
        for ds in self._data_sources:
            if ds.name == name:
                raise ValueError(
                    "Local data store '%s' already contains a data source named '%s'" % (self.name, name))
        data_source = LocalDataSource(name, files=[], data_store=self, config=None, reference_type=reference_type,
                                      reference_name=reference_name)
        data_source.save()
        self._data_sources.append(data_source)
        return data_source

    @property
    def data_store_path(self):
        return self._store_dir

    def query(self, name=None, monitor: Monitor = Monitor.NONE) -> Sequence[LocalDataSource]:
        self._init_data_sources()
        if name:
            result = [ds for ds in self._data_sources if ds.matches_filter(name)]
        else:
            result = self._data_sources
        return result

    def __repr__(self):
        return "LocalFilePatternDataStore(%s)" % repr(self.name)

    def _repr_html_(self):
        self._init_data_sources()
        rows = []
        row_count = 0
        for data_source in self._data_sources:
            row_count += 1
            # noinspection PyProtectedMember
            rows.append('<tr><td><strong>%s</strong></td><td>%s</td></tr>' % (row_count, data_source._repr_html_()))
        return '<p>Contents of LocalFilePatternDataStore "%s"</p><table>%s</table>' % (self.name, '\n'.join(rows))

    def _init_data_sources(self):
        if self._data_sources:
            return
        os.makedirs(self._store_dir, exist_ok=True)
        json_files = [f for f in os.listdir(self._store_dir)
                      if os.path.isfile(os.path.join(self._store_dir, f)) and f.endswith('.json')]
        self._data_sources = []
        for json_file in json_files:
            data_source = self._load_data_source(os.path.join(self._store_dir, json_file))
            if data_source:
                self._data_sources.append(data_source)

    def _load_data_source(self, json_path):
        json_dict = self._load_json_file(json_path)
        if json_dict:
            return LocalDataSource.from_json_dict(json_dict, self)

    @staticmethod
    def _load_json_file(json_path: str):
        if os.path.isfile(json_path):
            with open(json_path) as fp:
                return json.load(fp=fp) or {}
        return None
