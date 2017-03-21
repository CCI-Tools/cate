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

__author__ = "Marco Zühlke (Brockmann Consult GmbH)"

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
import shutil
import xarray as xr
from collections import OrderedDict
from datetime import datetime
from dateutil import parser
from glob import glob
from math import ceil, floor
from os import listdir, makedirs, environ
from os.path import basename, exists, isfile, join
from typing import Optional, Sequence, Tuple, Union, Any
from xarray.backends.netCDF4_ import NetCDF4DataStore

from cate.core.ds import DATA_STORE_REGISTRY, DataStore, DataSource, open_xarray_dataset
from cate.core.ds import get_data_stores_path
from cate.core.types import GeometryLike, TimeRange, TimeRangeLike, VariableNamesLike
from cate.util.misc import to_list
from cate.util.monitor import Monitor


def get_data_store_path():
    return environ.get('CATE_LOCAL_DATA_STORE_PATH',
                       join(get_data_stores_path(), 'local'))


def add_to_data_store_registry():
    data_store = LocalDataStore('local', get_data_store_path())
    DATA_STORE_REGISTRY.add_data_store(data_store)

from abc import ABCMeta, abstractmethod
from enum import Enum


class LocalDataSourceType(Enum):
    NONE = 0
    FILE_PATTERN = 1
    OPENDAP = 2


class LocalDataSourceConfiguration(metaclass=ABCMeta):

    def __init__(self, name: str, source: str, config_type: str,
                 data_store: DataStore):

        self._name = "{}.{}".format(data_store.name, name)
        self._source = source
        self._config_type = config_type
        self._files = OrderedDict()
        self._data_store = data_store

    def add_file(self, path: str, time_range: TimeRangeLike.TYPE = None, update: bool = False):

        is_disjoint = self._files.keys().isdisjoint([path])
        if not update and not is_disjoint:
            raise ValueError("Config already contains file `{}`".format(path))

        self._files[path] = time_range
        self._files = OrderedDict(sorted(self._files.items(), key=lambda f: f[1] if f[1] is not None else datetime.max))

    @abstractmethod
    def check_type(self, config_json: dict):
        return False

    @abstractmethod
    def save(self, path=None):
        pass


class FilePatternDataSourceConfiguration(LocalDataSourceConfiguration):

    def __init__(self, name: str, source: str, data_store: DataStore):
        super().__init__(name=name, source=source, config_type='FILE_PATTERN',
                         data_store=data_store)

    def check_type(self, config_json: dict):
        return self._config_type.lower() == config_json.get('type', 'unknown').lower()

    def save(self):
        config = OrderedDict()
        config['name'] = self._name
        config['type'] = self._config_type
        config['source'] = self._source
        config['files'] = [[item[0], item[1][0],item[1][1]] for item in self._files.items()]

        dump_kwargs = dict(indent='  ', default=self._json_default_serializer)
        file_name = join(self._data_store.data_store_path, self._name + '.json')
        with open(file_name, 'w') as fp:
            json.dump(config, fp, **dump_kwargs)

    @staticmethod
    def _json_default_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError('Not sure how to serialize %s' % (obj,))


class LocalDataSource(DataSource):
    def __init__(self, name: str, files: Union[Sequence[str], OrderedDict], data_store: DataStore,
                 config: LocalDataSourceConfiguration = None):
        self._name = name
        if isinstance(files, Sequence):
            self._files = OrderedDict.fromkeys(files)
        else:
            self._files = files
        self._data_store = data_store
        self._config = config if config else \
            FilePatternDataSourceConfiguration(name=name, source=None, data_store=data_store)

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
        if protocol:
            raise ValueError("Protocol '{}' is not recognized, use None for default")
        paths = []
        if time_range:
            time_series = list(self._files.values())
            file_paths = list(self._files.keys())
            for i in range(len(time_series)):
                if time_series[i] and time_range[0] <= time_series[i] < time_range[1]:
                    paths.extend(glob(file_paths[i]))
        else:
            for file in self._files.items():
                paths.extend(glob(file[0]))
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

    def make_local(self,
                   local_name: str,
                   local_id: str = None,
                   time_range: TimeRangeLike.TYPE = None,
                   region: GeometryLike.TYPE = None,
                   var_names: VariableNamesLike.TYPE = None,
                   monitor: Monitor = Monitor.NONE) -> 'DataSource':
        if not local_name:
            raise ValueError('local_name is required')
        elif len(local_name) == 0:
            raise ValueError('local_name cannot be empty')

        time_range = TimeRangeLike.convert(time_range) if time_range else None
        region = GeometryLike.convert(region) if region else None
        var_names = VariableNamesLike.convert(var_names) if var_names else None  # type: Sequence

        local_path = join(get_data_store_path(), local_name)

        makedirs(local_path, exist_ok=True)

        paths = []
        for file in self._files.items():
            paths.extend(glob(file[0]))
        paths = sorted(set(paths))
        monitor.start("make local", total_work=len(paths))
        if not paths:
            raise ValueError("Cannot make local copy of empty DataStore")

        config = FilePatternDataSourceConfiguration(
            name=local_name,
            source=self._name,
            data_store=self._data_store)

        for path in paths:
            child_monitor = monitor.child(work=1)

            remote_netcdf = NetCDF4DataStore(path)
            start_date = parser.parse(remote_netcdf.attrs['start_date'])
            stop_date = parser.parse(remote_netcdf.attrs['stop_date'])
            if start_date >= time_range[0] and stop_date <= time_range[1]:

                remote_dataset = xr.Dataset.load_store(remote_netcdf)

                local_netcdf = None

                file_name = basename(path)

                if region or var_names:
                    try:
                        local_filepath = join(local_path, file_name)

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
                    except:
                        raise
                    finally:
                        if remote_netcdf:
                            remote_netcdf.close()
                        if local_netcdf:
                            local_netcdf.close()
                else:
                    local_filepath = shutil.copy(src=path, dst=local_path)
                config.add_file(local_filepath, (start_date, stop_date))
            child_monitor.done()
        config.save()
        monitor.done()

    def add_dataset(self, file, time_stamp: datetime = None, update: bool = False):

        if update or self._files.keys().isdisjoint([file]):
            self._files[file] = time_stamp.replace(microsecond=0) if time_stamp else None
        self._files = OrderedDict(sorted(self._files.items(), key=lambda f: f[1] if f[1] is not None else datetime.max))

    def save(self):
        self._config.save()

    def temporal_coverage(self, monitor: Monitor = Monitor.NONE) -> Optional[TimeRange]:
        if self._files:
            cover_min = min(self._files.items(), key=lambda f: f[1] if f[1] is not None else datetime.max)[1]
            cover_max = max(self._files.items(), key=lambda f: f[1] if f[1] is not None else datetime.min)[1]
            if cover_min and cover_max:
                return cover_min, cover_max
        return None

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
                    files = OrderedDict((item[0], parser.parse(item[1]).replace(microsecond=0)
                    if item[1] is not None else None) for item in files)
            else:
                files = OrderedDict()
            return LocalDataSource(name, files, data_store)
        return None


class LocalDataStore(DataStore):
    def __init__(self, name: str, store_dir: str):
        super().__init__(name)
        self._store_dir = store_dir
        self._data_sources = None

    def add_pattern(self, name: str, files: Union[str, Sequence[str]]):
        self._init_data_sources()
        files = to_list(files, csv=False)

        if not name.startswith('%s.' % self.name):
            name = '%s.%s' % (self.name, name)
        for ds in self._data_sources:
            if ds.name == name:
                raise ValueError(
                    "Local data store '%s' already contains a data source named '%s'" % (self.name, name))
        data_source = LocalDataSource(name, files, self)
        data_source.save()

        self._data_sources.append(data_source)

        return name

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
        makedirs(self._store_dir, exist_ok=True)
        json_files = [f for f in listdir(self._store_dir) if isfile(join(self._store_dir, f)) and f.endswith('.json')]
        self._data_sources = []
        for json_file in json_files:
            data_source = self._load_data_source(join(self._store_dir, json_file))
            if data_source:
                self._data_sources.append(data_source)

    def _load_data_source(self, json_path):
        json_dict = self._load_json_file(json_path)
        if json_dict:
            return LocalDataSource.from_json_dict(json_dict, self)

    @staticmethod
    def _load_json_file(json_path: str):
        if isfile(json_path):
            with open(json_path) as fp:
                return json.load(fp=fp) or {}
        return None
