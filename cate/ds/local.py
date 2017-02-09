# The MIT License (MIT)
# Copyright (c) 2016 by the Cate Development Team and contributors
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
from typing import Sequence, Tuple, Union

import xarray as xr

from cate.core.ds import DATA_STORE_REGISTRY, DataStore, DataSource, open_xarray_dataset
from cate.core.ds import get_data_stores_path
from cate.util.misc import to_list
from cate.util.monitor import Monitor


def get_data_store_path():
    return environ.get('CATE_LOCAL_DATA_STORE_PATH',
                       join(get_data_stores_path(), 'local'))


def add_to_data_store_registry():
    data_store = LocalFilePatternDataStore('local', get_data_store_path())
    DATA_STORE_REGISTRY.add_data_store(data_store)


class LocalFilePatternDataSource(DataSource):
    def __init__(self, name: str, files: Sequence[str], _data_store: DataStore):
        self._name = name
        self._files = files
        self._data_store = _data_store

    def open_dataset(self, time_range: Tuple[datetime, datetime] = None,
                     protocol: str = None) -> xr.Dataset:
        if time_range:
            raise ValueError(
                "Local data store '%s' does not (yet) support temporal data subsets." % self._data_store.name)
        paths = []
        for file in self._files:
            paths.extend(glob(file))
        paths = sorted(set(paths))
        return open_xarray_dataset(paths)

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
        fsds_dict['files'] = self._files
        return fsds_dict

    @classmethod
    def from_json_dict(cls, json_dicts: dict, data_store: DataStore) -> 'LocalFilePatternDataSource':
        name = json_dicts.get('name')
        files = json_dicts.get('files')
        if name and files:
            return LocalFilePatternDataSource(name, files, data_store)
        return None


class LocalFilePatternDataStore(DataStore):
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
        data_source = LocalFilePatternDataSource(name, files, self)
        self._data_sources.append(data_source)
        self._save_data_source(data_source)
        return name

    def query(self, name=None, monitor: Monitor = Monitor.NONE) -> Sequence[DataSource]:
        self._init_data_sources()
        return [ds for ds in self._data_sources if ds.matches_filter(name)]

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
            return LocalFilePatternDataSource.from_json_dict(json_dict, self)

    def _save_data_source(self, data_source):
        json_dict = data_source.to_json_dict()
        dump_kwargs = dict(indent='  ')
        file_name = join(self._store_dir, data_source.name + '.json')
        with open(file_name, 'w') as fp:
            json.dump(json_dict, fp, **dump_kwargs)

    @staticmethod
    def _load_json_file(json_path: str):
        if isfile(json_path):
            with open(json_path) as fp:
                return json.load(fp=fp) or {}
        return None
