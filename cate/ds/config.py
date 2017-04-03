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

__author__ = "Chris Bernat (Telespazio VEGA UK Ltd)"

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
from abc import ABCMeta
from typing import Optional, Union, Sequence
from collections import OrderedDict
from datetime import datetime
from dateutil import parser

from cate.core.ds import DATA_STORE_REGISTRY, DataStore
from cate.core.types import GeometryLike, TimeRangeLike, VariableNamesLike


class LocalDataSourceConfiguration(metaclass=ABCMeta):

    def __init__(self, name: str, data_store: Union[DataStore, str], config_type: str, source: str = None,
                 temporal_coverage: TimeRangeLike.TYPE = None, region: GeometryLike.TYPE = None,
                 var_names: VariableNamesLike.TYPE = None,
                 last_update: datetime = None, files: OrderedDict = OrderedDict()):

        self._name = name
        self._temporal_coverage = TimeRangeLike.convert(temporal_coverage) if temporal_coverage else None
        self._region = GeometryLike.convert(region) if region else None
        self._var_names = VariableNamesLike.convert(var_names) if var_names else None

        self._source = source
        self._config_type = config_type
        self._last_update = last_update
        self._files = files
        if isinstance(data_store, DataStore):
            self._filename = "{}.{}".format(data_store.name, name)
            self._data_store = data_store
        else:
            self._filename = "{}.{}".format(data_store, name)
            self._data_store = DATA_STORE_REGISTRY.get_data_store(data_store)



    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._config_type

    @property
    def source(self):
        return self._source

    @property
    def temporal_coverage(self):
        return self._temporal_coverage

    @property
    def region(self):
        return self._region

    @property
    def var_names(self):
        return self._var_names

    @property
    def files(self):
        return self._files

    def add_file(self, path: str, time_range: TimeRangeLike.TYPE = None, update: bool = False):

        is_disjoint = self._files.keys().isdisjoint([path])
        if not update and not is_disjoint:
            raise ValueError("Config already contains file `{}`".format(path))

        self._files[path] = time_range
        if time_range:
            if self._temporal_coverage:
                if time_range[0] >= self._temporal_coverage[1]:
                    self._temporal_coverage = tuple([self._temporal_coverage[0], time_range[1]])
                elif time_range[1] <= self._temporal_coverage[0]:
                    self._temporal_coverage = tuple([time_range[0], self._temporal_coverage[1]])
            else:
                self._temporal_coverage = time_range

        self._files = OrderedDict(sorted(self._files.items(), key=lambda f: f[1] if f and f[1] else datetime.max))

    def remove_file(self, path: str):
        is_disjoint = self._files.keys().isdisjoint([path])
        if is_disjoint:
            raise ValueError("Config does not contains file `{}`".format(path))
        item = self._files.pop(path)
        values = list(self._files.values())
        if item and self._temporal_coverage:
            if item[0] == self._temporal_coverage[0]:
                self._temporal_coverage = tuple([values[0][0], self._temporal_coverage[1]])
            if item[1] == self._temporal_coverage[1]:
                self._temporal_coverage = tuple([self._temporal_coverage[0], values[len(values)-1][1]])

    def save(self):
        config = OrderedDict({
            'name': self._name,
            'meta_data': {
                'type': self._config_type,
                'data_store': self._data_store.name,
                'temporal_coverage': self._temporal_coverage,
                'spatial_coverage': self._region,
                'variables': self._var_names,
                'source': self._source,
                'last_update': None
            },
            'files': [[item[0], item[1][0], item[1][1]] for item in self._files.items()]
        })
        dump_kwargs = dict(indent='  ', default=self._json_default_serializer)
        file_name = os.path.join(self._data_store.data_store_path,
                                 "{}.{}.json".format(self._data_store.name, self.name))
        with open(file_name, 'w') as fp:
            json.dump(config, fp, **dump_kwargs)

    @staticmethod
    def load(json_file: Union[str, dict], data_store: Union[str, DataStore] = 'local') -> Optional['LocalDataSourceConfiguration']:

        config = None

        if isinstance(data_store, str):
            data_store = DATA_STORE_REGISTRY.get_data_store(data_store)

        if isinstance(json_file, str):
            json_file_name = json_file
            if json_file_name.count('.') > 0:

                primary_data_store_name, _ = json_file_name.split('.', 1)
                data_store = DATA_STORE_REGISTRY.get_data_store(primary_data_store_name)

            if data_store:
                json_file = os.path.join(data_store.data_store_path,
                                         "{}.{}.json".format(data_store.name, json_file))
                if os.path.isfile(json_file):
                    with open(json_file) as fp:
                        config = json.load(fp=fp) or None
        elif isinstance(json_file, dict):
            config = json_file
        if config:
            datasource_name = config.get('name')
            files = config.get('files', [])
            if files:
                if isinstance(files[0], str):
                    files = OrderedDict([(item, None) for item in files])
                elif isinstance(files[0], Sequence):
                    file_sequence_length = len(files[0])
                    if file_sequence_length > 2:
                        files = OrderedDict([(item[0], TimeRangeLike.convert(item[1] + ", " + item[2])
                                if item[1] and item[2] else None) for item in config.get('files', [])])
                    elif file_sequence_length > 1:
                        files = OrderedDict([(item[0], TimeRangeLike.convert(item[1] + ", " + item[1])
                        if item[1] else None) for item in config.get('files', [])])
                    elif file_sequence_length > 0:
                        files = OrderedDict([(item[0], None) for item in config.get('files', [])])
                else:
                    raise ValueError("cannot extract files from config")

            meta_data = config.get('meta_data', {})
            config_type = meta_data.get('type')
            datastore_name = meta_data.get('data_store')
            initial_temporal_coverage = meta_data.get('temporal_coverage', None)
            spatial_coverage = meta_data.get('spatial_coverage', None)
            variables = meta_data.get('variables', [])
            source = meta_data.get('source', None)
            last_update = meta_data.get('last_update', None)
            if initial_temporal_coverage and not isinstance(initial_temporal_coverage, str):
                initial_temporal_coverage = tuple(initial_temporal_coverage)

            return LocalDataSourceConfiguration(datasource_name, datastore_name, config_type, source,
                                                initial_temporal_coverage, spatial_coverage, variables, last_update,
                                                files)
        return None

    @staticmethod
    def _json_default_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError('Not sure how to serialize %s' % (obj,))

    @staticmethod
    def check_type(config_json: dict, expected_type: str):
        return expected_type.lower() == config_json.get('type', 'unknown').lower()
