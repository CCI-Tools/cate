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
from typing import Optional, Union
from collections import OrderedDict
from datetime import datetime
from dateutil import parser

from cate.core.ds import DATA_STORE_REGISTRY, DataStore
from cate.core.types import GeometryLike, TimeRangeLike, VariableNamesLike


class LocalDataSourceConfiguration(metaclass=ABCMeta):

    def __init__(self, name: str, data_store: Union[DataStore, str], config_type: str, source: str = None,
                 temporal_coverage: TimeRangeLike.TYPE = None, region: GeometryLike.TYPE = None,
                 var_names: VariableNamesLike.TYPE = None,
                 last_update: datetime = None, last_source_update: datetime=None):

        temporal_coverage = TimeRangeLike.convert(temporal_coverage) if temporal_coverage else None
        region = GeometryLike.convert(region) if region else None
        var_names = VariableNamesLike.convert(var_names) if var_names else None

        self._name = name
        self._source = source
        self._config_type = config_type
        self._files = OrderedDict()
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
    def files(self):
        return self._files

    def add_file(self, path: str, time_range: TimeRangeLike.TYPE = None, update: bool = False):

        is_disjoint = self._files.keys().isdisjoint([path])
        if not update and not is_disjoint:
            raise ValueError("Config already contains file `{}`".format(path))

        self._files[path] = time_range
        self._files = OrderedDict(sorted(self._files.items(), key=lambda f: f[1] if f[1] is not None else datetime.max))

    def remove_file(self, path: str):
        is_disjoint = self._files.keys().isdisjoint([path])
        if is_disjoint:
            raise ValueError("Config does not contains file `{}`".format(path))
        self._files.pop(path)

    def save(self):
        config = OrderedDict({
            'name': self._name,
            'meta_data': {
                'type': self._config_type,
                'data_store': self._data_store.name,
                'temporal_coverage': None,
                'spatial_coverage': None,
                'variables': [],
                'source': self._source,
                'last_update': None,
                'last_source_update': None
            },
            'files': [[item[0], item[1][0], item[1][1]] for item in self._files.items()]
        })
        dump_kwargs = dict(indent='  ', default=self._json_default_serializer)
        file_name = os.path.join(self._data_store.data_store_path, self._name + '.json')
        with open(file_name, 'w') as fp:
            json.dump(config, fp, **dump_kwargs)

    @staticmethod
    def load(json_path: str) -> Optional['LocalDataSourceConfiguration']:
        config = None
        if os.path.isfile(json_path):
            with open(json_path) as fp:
                config = json.load(fp=fp) or None
        if config:
            datasource_name = config.get('name')
            files = OrderedDict([(item[0], TimeRangeLike.convert(item[1]+", "+item[2])
                                 if item[1] and item[2] else None) for item in config.get('files', [])])
            meta_data = config.get('meta_data', {})
            config_type = meta_data.get('type')
            datastore_name = meta_data.get('data_store')
            initial_temporal_coverage = meta_data.get('temporal_coverage', None)
            spatial_coverage = meta_data.get('spatial_coverage', None)
            variables = meta_data.get('variables', [])
            source = meta_data.get('source', None)
            last_update = meta_data.get('last_update', None)
            last_source_update = meta_data.get('last_source_update', None)

            return LocalDataSourceConfiguration(datasource_name, datastore_name, config_type, source,
                                                initial_temporal_coverage, spatial_coverage, variables, last_update,
                                                last_source_update)
        return None

    @staticmethod
    def _json_default_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError('Not sure how to serialize %s' % (obj,))

    @staticmethod
    def check_type(config_json: dict, expected_type: str):
        return expected_type.lower() == config_json.get('type', 'unknown').lower()
