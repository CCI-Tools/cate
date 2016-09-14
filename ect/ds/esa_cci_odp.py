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

This plugin module adds the ESA CCI Open Data Portal's (ODP) ESGF service to
the data store registry.

Verification
============

The module's unit-tests are located in
`test/ds/test_esa_cci_odp.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ds/test_esa_cci_ftp.py>`_
and may be executed using
``$ py.test test/ds/test_esa_cci_odp.py --cov=ect/ds/esa_cci_odp.py`` for extra code coverage information.

Components
==========
"""

import json
import os
import os.path
import urllib.request
from datetime import datetime
from typing import Sequence

import xarray as xr
from ect.core.io import DATA_STORE_REGISTRY, DataStore, DataSource, Schema, TimeRange
from ect.core.monitor import Monitor

_ESGF_DATASET_URL = "https://esgf-index1.ceda.ac.uk/esg-search/search/?" \
                    "type=Dataset&" \
                    "offset=0&" \
                    "limit=10000&" \
                    "replica=false&" \
                    "latest=true&" \
                    "project=esacci&" \
                    "format=application%2Fsolr%2Bjson"

_ESGF_FILE_URL = "http://esgf-index1.ceda.ac.uk/esg-search/search/?" \
                 "type=File&" \
                 "offset=0&" \
                 "limit=10000&" \
                 "replica=false&" \
                 "fields=url%2Ctitle&" \
                 "dataset_id={dataset_id}&" \
                 "format=application%2Fsolr%2Bjson"

_DATA_SOURCES_DIR = os.path.expanduser(os.path.join('~', '.ect', 'data_stores'))
_DATA_ROOT = os.path.join(_DATA_SOURCES_DIR, 'esa_cci_odp')
_INDEX_CACHE_FILE = os.path.join(_DATA_ROOT, 'esgf-index-cache.json')
_INDEX_TIMESTAMP_FILE = os.path.join(_DATA_ROOT, 'esgf-index-timestamp.txt')
_INDEX_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


class EsaCciOdpDataStore(DataStore):
    def __init__(self,
                 index_cache_used: bool = True,
                 index_cache_expiration_days: float = 1.0,
                 index_cache_json_dict: dict = None):
        super(EsaCciOdpDataStore, self).__init__()
        self._index_cache_used = index_cache_used
        self._index_cache_expiration_days = index_cache_expiration_days
        self._index_cache_json_dict = index_cache_json_dict
        self._data_sources = []

    def query(self, name: str = None) -> Sequence['DataSource']:
        self._init_data_sources()
        if not name:
            return list(self._data_sources)
        return [data_source for data_source in self._data_sources if data_source.matches_filter(name)]

    def _repr_html_(self) -> str:
        self._init_data_sources()
        return ""

    def _init_data_sources(self) -> str:
        if self._data_sources:
            return
        self._load_index()
        response = self._index_cache_json_dict.get('response')
        num_found = response.get('numFound')
        docs = response.get('docs')
        self._data_sources = []
        for doc in docs:
            master_id = doc.get('master_id', None)
            self._data_sources.append(OdpDataSource(self, master_id, doc))

    def _load_index(self) -> str:
        if self._index_cache_json_dict is not None:
            return

        if self._index_cache_used:
            timestamp = datetime(year=2000, month=1, day=1)
            if os.path.exists(_INDEX_TIMESTAMP_FILE):
                with open(_INDEX_TIMESTAMP_FILE) as fp:
                    timestamp_text = fp.read()
                    timestamp = datetime.strptime(timestamp_text, _INDEX_TIMESTAMP_FORMAT)

            time_diff = datetime.now() - timestamp
            time_diff_days = time_diff.days + time_diff.seconds / 3600. / 24.
            if time_diff_days < self._index_cache_expiration_days:
                if os.path.exists(_INDEX_CACHE_FILE):
                    with open(_INDEX_CACHE_FILE) as fp:
                        json_text = fp.read()
                        self._index_cache_json_dict = json.loads(json_text)

        if self._index_cache_json_dict is None:
            with urllib.request.urlopen(_ESGF_DATASET_URL, timeout=10) as response:
                json_text = response.read()
                json_text = json_text.decode('utf-8')
                self._index_cache_json_dict = json.loads(json_text)

                if self._index_cache_used:
                    os.makedirs(_DATA_ROOT, exist_ok=True)
                    with open(_INDEX_CACHE_FILE, 'w') as fp:
                        fp.write(json_text)
                    with open(_INDEX_TIMESTAMP_FILE, 'w') as fp:
                        fp.write(datetime.utcnow().strftime(_INDEX_TIMESTAMP_FORMAT))


class OdpDataSource(DataSource):
    def __init__(self,
                 data_store: DataStore,
                 master_id: str,
                 json_dict: dict,
                 schema: Schema = None):
        super(OdpDataSource, self).__init__()
        self._master_id = master_id
        self._data_store = data_store
        self._json_dict = json_dict
        self._schema = schema

    @property
    def name(self) -> str:
        return self._master_id

    @property
    def data_store(self) -> DataStore:
        return self._data_store

    @property
    def schema(self) -> Schema:
        return self._schema

    @property
    def info_string(self):
        info_field_names = sorted(["realization",
                                   "project",
                                   "number_of_aggregations",
                                   "size",
                                   "product_string",
                                   "platform_id",
                                   "number_of_files",
                                   "product_version",
                                   "time_frequency",
                                   "processing_level",
                                   "sensor_id",
                                   "version",
                                   "cci_project",
                                   "data_type",
                                   ])
        max_len = 0
        for name in info_field_names:
            max_len = max(max_len, len(name))

        title = 'Data source "%s"' % self.name
        info_lines = [title,
                      '=' * len(title),
                      '']
        for name in info_field_names:
            value = self._json_dict[name]
            # Many values in the index JSON are one-element lists: not very helpful for human readers
            if isinstance(value, list) and len(value) == 1:
                value = value[0]
            info_lines.append('%s:%s %s' % (name, (max_len - len(name)) * ' ', value))

        return '\n'.join(info_lines)

    def matches_filter(self, name: str = None) -> bool:
        return name.lower() in self.name.lower()

    def find_url(self, desired_service='HTTP'):
        for url_service in self._json_dict.get('url', []):
            parts = url_service.rsplit('|', maxsplit=1)
            if len(parts) == 2:
                url, service = parts
                if service == desired_service:
                    return url
        return None

    def sync(self, time_range: TimeRange = (None, None), monitor: Monitor = Monitor.NULL):
        http_url = self.find_url()
        raise NotImplementedError

    def open_dataset(self, time_range: TimeRange = None) -> xr.Dataset:
        raise NotImplementedError

    def _repr_html_(self):
        return self.info_string

    def __str__(self):
        return self.info_string


def set_default_data_store():
    """
    Defines the ESA CCI ODP data store and makes it the default data store.

    All data sources of the FTP data store are read from a JSON file ``esa_cci_ftp.json`` contained in this package.
    This JSON file has been generated from a scan of the entire FTP tree.
    """
    DATA_STORE_REGISTRY.add_data_store('default', EsaCciOdpDataStore())
