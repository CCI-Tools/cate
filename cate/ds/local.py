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
from collections import OrderedDict
from datetime import datetime
from glob import glob
from os import listdir, makedirs, environ
from os.path import join, isfile
from typing import Optional, Sequence, Tuple, Union, Any

from dateutil import parser

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


class LocalDataSource(DataSource):
    def __init__(self, name: str, files: Union[Sequence[str], OrderedDict], _data_store: DataStore):
        self._name = name
        if isinstance(files, Sequence):
            self._files = OrderedDict.fromkeys(files)
        else:
            self._files = files
        self._data_store = _data_store

    def open_dataset(self,
                     time_range: TimeRangeLike.TYPE = None,
                     region: GeometryLike.TYPE = None,
                     var_names: VariableNamesLike.TYPE = None,
                     protocol: str = None) -> Any:
        time_range = TimeRangeLike.convert(time_range) if time_range else None
        # TODO (kbernat): support region constraint here
        if region:
            raise NotImplementedError('LocalDataSource.open_dataset() '
                                      'does not yet support the "region" constraint')
        # TODO (kbernat): support var_names constraint here
        if var_names:
            raise NotImplementedError('LocalDataSource.open_dataset() '
                                      'does not yet support the "var_names" constraint')
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
            return open_xarray_dataset(paths)
        else:
            return None

    def make_local(self,
                   local_name: str,
                   local_id: str = None,
                   time_range: TimeRangeLike.TYPE = None,
                   region: GeometryLike.TYPE = None,
                   var_names: VariableNamesLike.TYPE = None,
                   monitor: Monitor = Monitor.NONE) -> 'DataSource':
        # TODO (kbernat): implement me!
        raise NotImplementedError('LocalDataSource.make_local() '
                                  'is not yet implemented')

    def add_dataset(self, file, time_stamp: datetime = None, update: bool = False):
        if update or list(self._files.keys()).count(file) == 0:
            self._files[file] = time_stamp.replace(microsecond=0) if time_stamp else None
        self._files = OrderedDict(sorted(self._files.items(), key=lambda f: f[1] if f[1] is not None else datetime.max))

    def save(self):
        self._data_store.save_data_source(self)

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
        self._data_sources.append(data_source)
        self._save_data_source(data_source)
        return name

    def query(self, name=None, monitor: Monitor = Monitor.NONE) -> Sequence[LocalDataSource]:
        self._init_data_sources()
        if name:
            result = [ds for ds in self._data_sources if ds.matches_filter(name)]
        else:
            result = self._data_sources
        return result

    def save_data_source(self, data_source):
        self._save_data_source(data_source)

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

    def _save_data_source(self, data_source):
        json_dict = data_source.to_json_dict()
        dump_kwargs = dict(indent='  ', default=self._json_default_serializer)
        file_name = join(self._store_dir, data_source.name + '.json')
        with open(file_name, 'w') as fp:
            json.dump(json_dict, fp, **dump_kwargs)

    @staticmethod
    def _load_json_file(json_path: str):
        if isfile(json_path):
            with open(json_path) as fp:
                return json.load(fp=fp) or {}
        return None

    @staticmethod
    def _json_default_serializer(obj):
        if isinstance(obj, datetime):
            return obj.replace(microsecond=0).isoformat()
        raise TypeError('Not sure how to serialize %s' % (obj,))
