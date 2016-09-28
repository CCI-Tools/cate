# The MIT License (MIT)
# Copyright (c) 2016 by the ECT Development Team and contributors
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

"""
Description
===========

This plugin module adds the local data source to the data store registry.

Verification
============

The module's unit-tests are located in
`test/ds/test_esa_cci_ftp.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ds/test_esa_cci_ftp.py>`_
and may be executed using ``$ py.test test/ds/test_esa_cci_ftp.py --cov=ect/ds/esa_cci_ftp.py``
for extra code coverage information.

Components
==========
"""
import json
import os
from collections import OrderedDict
from datetime import datetime
from typing import Sequence, Tuple

import xarray as xr
from ect.core.io import DATA_STORE_REGISTRY, DataStore, DataSource, open_xarray_dataset
from ect.core.monitor import Monitor


_DATA_SOURCES_DIR = os.path.expanduser(os.path.join('~', '.ect', 'data_stores'))
_LOCAL_JSON = os.path.join(_DATA_SOURCES_DIR, 'local.json')


def add_to_data_store():
    data_store = LocalFilePatternDataStore.from_json_file(_LOCAL_JSON)
    DATA_STORE_REGISTRY.add_data_store(data_store)


class LocalFilePatternDataSource(DataSource):
    def __init__(self, name: str, file_pattern: str, _data_store: DataStore):
        self._name = name
        self._file_pattern = file_pattern
        self._data_store = _data_store

    def open_dataset(self, time_range: Tuple[datetime, datetime] = None) -> xr.Dataset:
        return open_xarray_dataset(self._file_pattern)

    @property
    def info_string(self):
        table_data = self.get_table_data()
        return '\n'.join(['%s: %s' % (name, value)
                          for name, value in table_data.items()])

    def _repr_html_(self):
        import html
        table_data = self.get_table_data()
        rows = '\n'.join(['<tr><td>%s</td><td><strong>%s</strong></td></tr>' % (name, html.escape(str(value)))
                          for name, value in table_data.items()])
        return '<table style="border:0;">%s</table>' % rows

    def get_table_data(self):
        return OrderedDict([('Name', self._name),
                            ('File pattern', self._file_pattern)])

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
        fsds_dict['file_pattern'] = self._file_pattern
        return fsds_dict

    @classmethod
    def from_json_dict(cls, json_dicts: dict, data_store: DataStore) -> 'LocalFilePatternDataSource':
        name = json_dicts.get('name')
        file_pattern = json_dicts.get('file_pattern')
        if name and file_pattern:
            return LocalFilePatternDataSource(name, file_pattern, data_store)
        return None


class LocalFilePatternDataStore(DataStore):
    def __init__(self, name: str, json_path: str):
        super().__init__(name)
        self.json_path = json_path
        self._data_sources = []

    def add_pattern(self, name: str, file_pattern: str):
        if not name.startswith('%s.' % self.name):
            name = '%s.%s' % (self.name, name)
        for ds in self._data_sources:
            if ds.name == name:
                raise ValueError("The data_store already contains a data_source with the name '%s'" % name)
        self._data_sources.append(LocalFilePatternDataSource(name, file_pattern, self))
        self.store_to_json()
        return name

    def query(self, name=None, monitor: Monitor = Monitor.NONE) -> Sequence[DataSource]:
        return [ds for ds in self._data_sources if ds.matches_filter(name)]

    def __repr__(self):
        return "LocalFilePatternDataStore(%s)" % repr(self.name)

    def _repr_html_(self):
        rows = []
        row_count = 0
        for data_source in self._data_sources:
            row_count += 1
            # noinspection PyProtectedMember
            rows.append('<tr><td><strong>%s</strong></td><td>%s</td></tr>' % (row_count, data_source._repr_html_()))
        return '<p>Contents of LocalFilePatternDataStore "%s"</p><table>%s</table>' % (self.name, '\n'.join(rows))

    @classmethod
    def from_json_file(cls, json_path: str) -> 'LocalFilePatternDataStore':
        local_data_store = LocalFilePatternDataStore('local', json_path)
        local_data_store.load_from_json(json_path)
        return local_data_store

    def load_from_json(self, json_path):
        json_dict = self._load_json_file(json_path)
        if json_dict:
            data_sources_list = json_dict.get('data_sources', [])
            for ds_dict in data_sources_list:
                data_source = LocalFilePatternDataSource.from_json_dict(ds_dict, self)
                if data_source:
                    self._data_sources.append(data_source)

    def store_to_json(self):
        json_dict = self.to_json_dict()
        dump_kwargs = dict(indent='  ')
        with open(self.json_path, 'w') as fp:
            json.dump(json_dict, fp, **dump_kwargs)

    def to_json_dict(self):
        """
        Return a JSON-serializable dictionary representation of this object.

        :return: A JSON-serializable dictionary
        """
        ds_json_list = []
        for data_source in self._data_sources:
            ds_json_list.append(data_source.to_json_dict())

        json_dict = OrderedDict()
        json_dict['data_sources'] = ds_json_list
        return json_dict

    @staticmethod
    def _load_json_file(json_path: str):
        if os.path.isfile(json_path):
            with open(json_path) as fp:
                return json.load(fp=fp) or {}
        return None
