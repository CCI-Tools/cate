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
from math import ceil, floor, isnan
from typing import Optional, Sequence, Union, Any, Tuple
from xarray.backends import NetCDF4DataStore

from cate.conf import get_config_value
from cate.conf.defaults import NETCDF_COMPRESSION_LEVEL
from cate.core.ds import DATA_STORE_REGISTRY, DataStore, DataSource, open_xarray_dataset, query_data_sources
from cate.core.ds import get_data_stores_path
from cate.core.types import PolygonLike, TimeRange, TimeRangeLike, VarNamesLike
from cate.util.monitor import Monitor

_REFERENCE_DATA_SOURCE_TYPE = "FILE_PATTERN"


def get_data_store_path():
    return os.environ.get('CATE_LOCAL_DATA_STORE_PATH',
                          os.path.join(get_data_stores_path(), 'local'))


def add_to_data_store_registry():
    data_store = LocalDataStore('local', get_data_store_path())
    DATA_STORE_REGISTRY.add_data_store(data_store)


class LocalDataSource(DataSource):
    def __init__(self, name: str, files: Union[Sequence[str], OrderedDict], data_store: 'LocalDataStore',
                 temporal_coverage: TimeRangeLike.TYPE = None, spatial_coverage: PolygonLike.TYPE = None,
                 variables: VarNamesLike.TYPE = None, reference_type: str = None, reference_name: str = None):
        self._name = name
        if isinstance(files, Sequence):
            self._files = OrderedDict.fromkeys(files)
        else:
            self._files = files
        self._data_store = data_store

        initial_temporal_coverage = TimeRangeLike.convert(temporal_coverage) if temporal_coverage else None
        if not initial_temporal_coverage:
            files_number = len(self._files.items())
            if files_number > 0:
                files_range = list(self._files.values())
                if files_range:
                    if isinstance(files_range[0], Tuple):
                        initial_temporal_coverage = TimeRangeLike.convert(tuple([files_range[0][0],
                                                                                 files_range[files_number - 1][1]]))
                    elif isinstance(files_range[0], datetime):
                        initial_temporal_coverage = TimeRangeLike.convert((files_range[0],
                                                                           files_range[files_number - 1]))

        self._temporal_coverage = initial_temporal_coverage
        self._spatial_coverage = PolygonLike.convert(spatial_coverage) if spatial_coverage else None
        self._variables = VarNamesLike.convert(variables) if variables else None

        self._reference_type = reference_type if reference_type else None
        self._reference_name = reference_name

    def _resolve_file_path(self, path) -> Sequence:
        return glob(os.path.join(self._data_store.data_store_path, path))

    def open_dataset(self,
                     time_range: TimeRangeLike.TYPE = None,
                     region: PolygonLike.TYPE = None,
                     var_names: VarNamesLike.TYPE = None,
                     protocol: str = None) -> Any:
        time_range = TimeRangeLike.convert(time_range) if time_range else None
        if region:
            region = PolygonLike.convert(region)
        if var_names:
            var_names = VarNamesLike.convert(var_names)
        paths = []
        if time_range:
            time_series = list(self._files.values())
            file_paths = list(self._files.keys())
            for i in range(len(time_series)):
                if time_series[i]:
                    if isinstance(time_series[i], Tuple) and \
                            time_series[i][0] >= time_range[0] and \
                            time_series[i][1] <= time_range[1]:
                        paths.extend(self._resolve_file_path(file_paths[i]))
                    elif isinstance(time_series[i], datetime) and \
                            time_range[0] <= time_series[i] < time_range[1]:
                        paths.extend(self._resolve_file_path(file_paths[i]))
        else:
            for file in self._files.items():
                paths.extend(self._resolve_file_path(file[0]))
        if paths:
            paths = sorted(set(paths))
            try:
                ds = open_xarray_dataset(paths)
                if region:
                    [lon_min, lat_min, lon_max, lat_max] = region.bounds
                    ds = ds.sel(drop=False, lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max))
                if var_names:
                    ds = ds.drop([var_name for var_name in ds.variables.keys() if var_name not in var_names])
                return ds
            except OSError as e:
                raise IOError("Files: {} caused:\nOSError({}): {}".format(paths, e.errno, e.strerror))
        else:
            return None

    @staticmethod
    def _get_harmonized_coordinate_value(attrs: dict, attr_name: str):
        value = attrs.get(attr_name, 'nan')
        if isinstance(value, str):
            return float(value.rstrip('degrees').rstrip('f'))
        return value

    def _make_local(self,
                    local_ds: 'LocalDataSource',
                    time_range: TimeRangeLike.TYPE = None,
                    region: PolygonLike.TYPE = None,
                    var_names: VarNamesLike.TYPE = None,
                    monitor: Monitor = Monitor.NONE):

        # local_name = local_ds.name
        local_id = local_ds.name

        time_range = TimeRangeLike.convert(time_range) if time_range else None
        region = PolygonLike.convert(region) if region else None
        var_names = VarNamesLike.convert(var_names) if var_names else None  # type: Sequence

        compression_level = get_config_value('NETCDF_COMPRESSION_LEVEL', NETCDF_COMPRESSION_LEVEL)
        compression_enabled = True if compression_level > 0 else False

        encoding_update = dict()
        if compression_enabled:
            encoding_update.update({'zlib': True, 'complevel': compression_level})

        local_path = os.path.join(local_ds.data_store.data_store_path, local_id)
        data_store_path = local_ds.data_store.data_store_path
        if not os.path.exists(local_path):
            os.makedirs(local_path)

        monitor.start("Sync " + self.name, total_work=len(self._files.items()))
        for remote_relative_filepath, coverage in self._files.items():
            child_monitor = monitor.child(work=1)

            file_name = os.path.basename(remote_relative_filepath)
            local_relative_filepath = os.path.join(local_id, file_name)
            local_absolute_filepath = os.path.join(data_store_path, local_relative_filepath)

            remote_absolute_filepath = os.path.join(self._data_store.data_store_path, remote_relative_filepath)

            if isinstance(coverage, Tuple):

                time_coverage_start = coverage[0]
                time_coverage_end = coverage[1]

                remote_netcdf = None
                local_netcdf = None
                if not time_range or time_coverage_start >= time_range[0] and time_coverage_end <= time_range[1]:
                    if region or var_names:
                        try:
                            remote_netcdf = NetCDF4DataStore(remote_absolute_filepath)

                            local_netcdf = NetCDF4DataStore(local_absolute_filepath, mode='w', persist=True)
                            local_netcdf.set_attributes(remote_netcdf.get_attrs())

                            remote_dataset = xr.Dataset.load_store(remote_netcdf)

                            process_region = False
                            if region:
                                geo_lat_min = self._get_harmonized_coordinate_value(remote_dataset.attrs,
                                                                                    'geospatial_lat_min')
                                geo_lat_max = self._get_harmonized_coordinate_value(remote_dataset.attrs,
                                                                                    'geospatial_lat_max')
                                geo_lon_min = self._get_harmonized_coordinate_value(remote_dataset.attrs,
                                                                                    'geospatial_lon_min')
                                geo_lon_max = self._get_harmonized_coordinate_value(remote_dataset.attrs,
                                                                                    'geospatial_lon_max')

                                geo_lat_res = self._get_harmonized_coordinate_value(remote_dataset.attrs,
                                                                                    'geospatial_lon_resolution')
                                geo_lon_res = self._get_harmonized_coordinate_value(remote_dataset.attrs,
                                                                                    'geospatial_lat_resolution')
                                if not (isnan(geo_lat_min) or isnan(geo_lat_max) or isnan(geo_lon_min) or
                                        isnan(geo_lon_max) or isnan(geo_lat_res) or isnan(geo_lon_res)):
                                    process_region = True

                                    [lon_min, lat_min, lon_max, lat_max] = region.bounds

                                    descending_data_order = set()
                                    for var in remote_dataset.coords.keys():
                                        if remote_dataset.coords[var][0] > remote_dataset.coords[var][-1]:
                                            descending_data_order.add(var)

                                    if 'lat' not in descending_data_order:
                                        lat_min = lat_min - geo_lat_min
                                        lat_max = lat_max - geo_lat_min
                                    else:
                                        lat_min_copy = lat_min
                                        lat_min = geo_lat_max - lat_max
                                        lat_max = geo_lat_max - lat_min_copy

                                    if 'lon' not in descending_data_order:
                                        lon_min = lon_min - geo_lon_min
                                        lon_max = lon_max - geo_lon_min
                                    else:
                                        lon_min_copy = lon_min
                                        lon_min = geo_lon_max - lon_max
                                        lon_max = geo_lon_max - lon_min_copy

                                    lat_min = floor(lat_min / geo_lat_res)
                                    lat_max = ceil(lat_max / geo_lat_res)
                                    lon_min = floor(lon_min / geo_lon_res)
                                    lon_max = ceil(lon_max / geo_lon_res)

                                    remote_dataset = remote_dataset.isel(drop=False,
                                                                         lat=slice(lat_min, lat_max),
                                                                         lon=slice(lon_min, lon_max))
                                    if 'lat' not in descending_data_order:
                                        geo_lat_min_copy = geo_lat_min
                                        geo_lat_min = lat_min * geo_lat_res + geo_lat_min_copy
                                        geo_lat_max = lat_max * geo_lat_res + geo_lat_min_copy
                                    else:
                                        geo_lat_max_copy = geo_lat_max
                                        geo_lat_min = geo_lat_max_copy - lat_max * geo_lat_res
                                        geo_lat_max = geo_lat_max_copy - lat_min * geo_lat_res

                                    if 'lon' not in descending_data_order:
                                        geo_lon_min_copy = geo_lon_min
                                        geo_lon_min = lon_min * geo_lon_res + geo_lon_min_copy
                                        geo_lon_max = lon_max * geo_lon_res + geo_lon_min_copy
                                    else:
                                        geo_lon_max_copy = geo_lon_max
                                        geo_lon_min = geo_lon_max_copy - lon_max * geo_lon_res
                                        geo_lon_max = geo_lon_max_copy - lon_min * geo_lon_res

                            if not var_names:
                                var_names = [var_name for var_name in remote_netcdf.variables.keys()]
                            var_names.extend([coord_name for coord_name in remote_dataset.coords.keys()
                                              if coord_name not in var_names])
                            child_monitor.start(label=file_name, total_work=len(var_names))
                            for sel_var_name in var_names:
                                var_dataset = remote_dataset.drop(
                                    [var_name for var_name in remote_dataset.variables.keys() if
                                     var_name != sel_var_name])
                                if compression_enabled:
                                    var_dataset.variables.get(sel_var_name).encoding.update(encoding_update)
                                local_netcdf.store_dataset(var_dataset)
                                child_monitor.progress(work=1, msg=sel_var_name)
                            if process_region:
                                local_netcdf.set_attribute('geospatial_lat_min', geo_lat_min)
                                local_netcdf.set_attribute('geospatial_lat_max', geo_lat_max)
                                local_netcdf.set_attribute('geospatial_lon_min', geo_lon_min)
                                local_netcdf.set_attribute('geospatial_lon_max', geo_lon_max)
                        finally:
                            if remote_netcdf:
                                remote_netcdf.close()
                            if local_netcdf:
                                local_netcdf.close()
                                local_ds.add_dataset(local_relative_filepath, (time_coverage_start, time_coverage_end))
                        child_monitor.done()
                    else:
                        shutil.copy(remote_absolute_filepath, local_absolute_filepath)
                        local_ds.add_dataset(local_relative_filepath, (time_coverage_start, time_coverage_end))
                        child_monitor.done()
        monitor.done()
        return local_id

    def make_local(self,
                   local_name: str,
                   local_id: str = None,
                   time_range: TimeRangeLike.TYPE = None,
                   region: PolygonLike.TYPE = None,
                   var_names: VarNamesLike.TYPE = None,
                   monitor: Monitor = Monitor.NONE) -> 'DataSource':
        if not local_name:
            raise ValueError('local_name is required')
        elif len(local_name) == 0:
            raise ValueError('local_name cannot be empty')

        local_store = DATA_STORE_REGISTRY.get_data_store('local')
        if not local_store:
            add_to_data_store_registry()
            local_store = DATA_STORE_REGISTRY.get_data_store('local')
        if not local_store:
            raise ValueError('Cannot initialize `local` DataStore')

        local_ds = local_store.create_data_source(local_name, region, _REFERENCE_DATA_SOURCE_TYPE, self.name)
        self._make_local(local_ds, time_range, region, var_names, monitor)
        return local_ds

    def update_local(self,
                     local_id: str,
                     time_range: TimeRangeLike.TYPE,
                     monitor: Monitor = Monitor.NONE) -> bool:

        data_sources = query_data_sources(None, local_id)  # type: Sequence['DataSource']
        data_source = next((ds for ds in data_sources if isinstance(ds, LocalDataSource) and
                            ds.name == local_id), None)  # type: LocalDataSource
        if not data_source:
            raise ValueError("Couldn't find local DataSource", (local_id, data_sources))

        time_range = TimeRangeLike.convert(time_range) if time_range else None

        to_remove = []
        to_add = []
        if time_range and time_range[1] > time_range[0]:
            if time_range[0] != data_source.temporal_coverage()[0]:
                if time_range[0] > data_source.temporal_coverage()[0]:
                    to_remove.append((data_source.temporal_coverage()[0], time_range[0]))
                else:
                    to_add.append((time_range[0], data_source.temporal_coverage()[0]))

            if time_range[1] != data_source.temporal_coverage()[1]:
                if time_range[1] < data_source.temporal_coverage()[1]:
                    to_remove.append((time_range[1], data_source.temporal_coverage()[1]))
                else:
                    to_add.append((data_source.temporal_coverage()[1],
                                   time_range[1]))
        if to_remove:
            for time_range_to_remove in to_remove:
                data_source.reduce_temporal_coverage(time_range_to_remove)
        if to_add:
            for time_range_to_add in to_add:
                self._make_local(data_source, time_range_to_add, None, data_source.variables_info, monitor)
        return bool(to_remove or to_add)

    def add_dataset(self, file, time_coverage: TimeRangeLike.TYPE = None, update: bool = False):
        if update or self._files.keys().isdisjoint([file]):
            self._files[file] = time_coverage
            if time_coverage:
                self._extend_temporal_coverage(time_coverage)
        self._files = OrderedDict(sorted(self._files.items(),
                                         key=lambda f: f[1] if isinstance(f, Tuple) and f[1] else datetime.max))
        self.save()

    def _extend_temporal_coverage(self, time_range: TimeRangeLike.TYPE):
        """

        :param time_range: Time range to be added to data source temporal coverage
        :return:
        """
        if not time_range:
            return
        if self._temporal_coverage:
            if time_range[0] >= self._temporal_coverage[1]:
                self._temporal_coverage = tuple([self._temporal_coverage[0], time_range[1]])
            elif time_range[1] <= self._temporal_coverage[0]:
                self._temporal_coverage = tuple([time_range[0], self._temporal_coverage[1]])
        else:
            self._temporal_coverage = time_range

    def _reduce_temporal_coverage(self, time_range: TimeRangeLike.TYPE):
        """

        :param time_range:Time range to be removed from data source temporal coverage
        :return:
        """
        if not time_range or not self._temporal_coverage:
            return
        if time_range[0] > self._temporal_coverage[0] and time_range[1] == self._temporal_coverage[1]:
            self._temporal_coverage = (self._temporal_coverage[0], time_range[0])
        if time_range[1] < self._temporal_coverage[1] and time_range[0] == self._temporal_coverage[0]:
            self._temporal_coverage = (time_range[1], self._temporal_coverage[1])

    def reduce_temporal_coverage(self, time_coverage: TimeRangeLike.TYPE):
        files_to_remove = []
        time_range_to_be_removed = None
        for file, time_range in self._files.items():
            if time_coverage[0] <= time_range[0] <= time_coverage[1] \
                    and time_coverage[0] <= time_range[1] <= time_coverage[1]:
                files_to_remove.append(file)
                if not time_range_to_be_removed and isinstance(time_range, Tuple):
                    time_range_to_be_removed = time_range
                else:
                    time_range_to_be_removed = (time_range_to_be_removed[0], time_range[1])
            elif time_coverage[0] <= time_range[0] <= time_coverage[1]:
                time_range_to_be_removed = (time_range_to_be_removed[0], time_range[0])
            elif time_coverage[0] <= time_range[1] <= time_coverage[1]:
                time_range_to_be_removed = time_range[1], time_coverage[1]
        for file in files_to_remove:
            os.remove(os.path.join(self._data_store.data_store_path, file))
            del self._files[file]
        if time_range_to_be_removed:
            self._reduce_temporal_coverage(time_range_to_be_removed)

    def save(self):
        self._data_store.save_data_source(self)

    def temporal_coverage(self, monitor: Monitor = Monitor.NONE) -> Optional[TimeRange]:
        return self._temporal_coverage

    def spatial_coverage(self):
        return self._spatial_coverage

    @property
    def variables_info(self):
        return self._variables

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
        config = OrderedDict({
            'name': self._name,
            'meta_data': {
                'temporal_coverage': TimeRangeLike.format(self._temporal_coverage) if self._temporal_coverage else None,
                'spatial_coverage': PolygonLike.format(self._spatial_coverage) if self._spatial_coverage else None,
                'variables': VarNamesLike.format(self._variables) if self._variables else None,

                'reference_type': self._reference_type,
                'reference_name': self._reference_name
            },
            'files': [[item[0], item[1][0], item[1][1]] if item[1] else [item[0]] for item in self._files.items()]
        })
        return config

    @classmethod
    def from_json_dict(cls, json_dicts: dict, data_store: 'LocalDataStore') -> Optional['LocalDataSource']:

        name = json_dicts.get('name')
        files = json_dicts.get('files', None)
        meta_data = json_dicts.get('meta_data', {})

        temporal_coverage = meta_data.get('temporal_coverage', None)
        # TODO why is this code here, doesn't work, because 'temporal_coverage' is a string
        # if temporal_coverage and isinstance(temporal_coverage, Sequence):
        #     temporal_coverage = tuple(temporal_coverage)

        spatial_coverage = meta_data.get('spatial_coverage', None)
        variables = meta_data.get('variables', None)

        reference_type = meta_data.get('reference_type', None)
        reference_name = meta_data.get('reference_name', None)

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
            return LocalDataSource(name, files, data_store, temporal_coverage, spatial_coverage, variables,
                                   reference_type, reference_name)
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

    def remove_data_source(self, name: str, remove_files: bool = True):
        data_sources = self.query(name)
        if not data_sources or len(data_sources) != 1:
            return
        data_source = data_sources[0]
        file_name = os.path.join(self._store_dir, data_source.name + '.json')
        os.remove(file_name)
        if remove_files:
            shutil.rmtree(os.path.join(self._store_dir, data_source.name))
        self._data_sources.remove(data_source)

    def create_data_source(self, name: str, region: PolygonLike.TYPE = None,
                           reference_type: str = None, reference_name: str = None):
        self._init_data_sources()
        if not name.startswith('%s.' % self.name):
            name = '%s.%s' % (self.name, name)
        for ds in self._data_sources:
            if ds.name == name:
                raise ValueError(
                    "Local data store '%s' already contains a data source named '%s'" % (self.name, name))
        data_source = LocalDataSource(name, files=[], data_store=self, spatial_coverage=region,
                                      reference_type=reference_type, reference_name=reference_name)
        self._save_data_source(data_source)
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

    def save_data_source(self, data_source):
        self._save_data_source(data_source)

    def _save_data_source(self, data_source):
        json_dict = data_source.to_json_dict()
        dump_kwargs = dict(indent='  ', default=self._json_default_serializer)
        file_name = os.path.join(self._store_dir, data_source.name + '.json')
        with open(file_name, 'w') as fp:
            json.dump(json_dict, fp, **dump_kwargs)

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

    @staticmethod
    def _json_default_serializer(obj):
        if isinstance(obj, datetime):
            return obj.replace(microsecond=0).isoformat()
        # if isinstance(obj, Polygon):
        #    return str(obj.bounds).replace(' ', '').replace('(', '\"').replace(')', '\"'))
        raise TypeError('Not sure how to serialize %s' % (obj,))
