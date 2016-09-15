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
import re
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Sequence, Tuple

import xarray as xr
from ect.core.io import DATA_STORE_REGISTRY, DataStore, DataSource, Schema, TimeRange
from ect.core.monitor import Monitor
from ect.core.util import to_datetime

_ESGF_CEDA_URL = "https://esgf-index1.ceda.ac.uk/esg-search/search/"

_DATA_SOURCES_DIR = os.path.expanduser(os.path.join('~', '.ect', 'data_stores'))
_DATA_ROOT = os.path.join(_DATA_SOURCES_DIR, 'esa_cci_odp')

_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

_RE_TO_DATETIME_FORMATS = patterns = [(re.compile(14 * '\\d'), '%Y%m%d%H%M%S'),
                                      (re.compile(12 * '\\d'), '%Y%m%d%H%M'),
                                      (re.compile(8 * '\\d'), '%Y%m%d'),
                                      (re.compile(6 * '\\d'), '%Y%m'),
                                      (re.compile(4 * '\\d'), '%Y')]


def set_default_data_store():
    """
    Defines the ESA CCI ODP data store and makes it the default data store.

    All data sources of the FTP data store are read from a JSON file ``esa_cci_ftp.json`` contained in this package.
    This JSON file has been generated from a scan of the entire FTP tree.
    """
    DATA_STORE_REGISTRY.add_data_store('default', EsaCciOdpDataStore())


def _find_datetime_format(filename: str) -> Tuple[str, int, int]:
    for regex, time_format in _RE_TO_DATETIME_FORMATS:
        searcher = regex.search(filename)
        if searcher:
            p1, p2 = searcher.span()
            return time_format, p1, p2
    return None, -1, -1


def _fetch_solr_json(base_url, query_args, offset=0, limit=10000, timeout=10):
    """
    Return JSON value read from paginated Solr web-service.
    """
    new_offset = offset
    combined_json_dict = None
    combined_num_found = 0
    while True:
        paging_query_args = dict(query_args or {})
        paging_query_args.update(offset=new_offset, limit=limit, format='application/solr+json')
        url = base_url + '?' + urllib.parse.urlencode(paging_query_args)
        with urllib.request.urlopen(url, timeout=timeout) as response:
            json_text = response.read()
            json_dict = json.loads(json_text.decode('utf-8'))
            num_found = json_dict.get('response', {}).get('numFound', 0)
            if not combined_json_dict:
                combined_json_dict = json_dict
                if num_found < limit:
                    break
            else:
                if num_found > 0:
                    combined_num_found += num_found
                    docs = json_dict.get('response', {}).get('docs', [])
                    combined_json_dict.get('response', {}).get('docs', []).append(docs)
                    combined_json_dict.get('response', {})['numFound'] = combined_num_found
                    if num_found < limit:
                        break
                else:
                    break
    return combined_json_dict


def _load_or_fetch_json(fetch_json_function,
                        fetch_json_args: list = None,
                        fetch_json_kwargs: dict = None,
                        cache_used: bool = False,
                        cache_dir: str = None,
                        cache_json_filename: str = None,
                        cache_timestamp_filename: str = None,
                        cache_expiration_days: float = 1.0) -> Sequence:
    """
    Return (JSON) value of fetch_json_function or return value of a cached JSON file.
    """
    json_obj = None

    if cache_used:
        if cache_dir is None:
            raise ValueError('if cache_used argument is True, cache_dir argument must not be None')
        if cache_json_filename is None:
            raise ValueError('if cache_used argument is True, cache_json_filename argument must not be None')
        if cache_timestamp_filename is None:
            raise ValueError('if cache_used argument is True, cache_timestamp_filename argument must not be None')
        if cache_expiration_days is None:
            raise ValueError('if cache_used argument is True, cache_expiration_days argument must not be None')

        cache_json_file = os.path.join(cache_dir, cache_json_filename)
        cache_timestamp_file = os.path.join(cache_dir, cache_timestamp_filename)

        timestamp = datetime(year=2000, month=1, day=1)
        if os.path.exists(cache_timestamp_file):
            with open(cache_timestamp_file) as fp:
                timestamp_text = fp.read()
                timestamp = datetime.strptime(timestamp_text, _TIMESTAMP_FORMAT)

        time_diff = datetime.now() - timestamp
        time_diff_days = time_diff.days + time_diff.seconds / 3600. / 24.
        if time_diff_days < cache_expiration_days:
            if os.path.exists(cache_json_file):
                with open(cache_json_file) as fp:
                    json_text = fp.read()
                    json_obj = json.loads(json_text)

    if json_obj is None:
        # noinspection PyArgumentList
        json_obj = fetch_json_function(*(fetch_json_args or []), **(fetch_json_kwargs or {}))
        if cache_used:
            os.makedirs(cache_dir, exist_ok=True)
            # noinspection PyUnboundLocalVariable
            with open(cache_json_file, 'w') as fp:
                fp.write(json.dumps(json_obj, indent='  '))
            # noinspection PyUnboundLocalVariable
            with open(cache_timestamp_file, 'w') as fp:
                fp.write(datetime.utcnow().strftime(_TIMESTAMP_FORMAT))

    return json_obj


def _fetch_file_list_json(dataset_id: str, dataset_query_id: str):
    file_index_json_dict = _fetch_solr_json(_ESGF_CEDA_URL,
                                            dict(type='File',
                                                 fields='url,title,size',
                                                 dataset_id=dataset_query_id,
                                                 replica='false',
                                                 latest='true',
                                                 project='esacci'))

    if not isinstance(file_index_json_dict, dict):
        return None

    file_list = []
    docs = file_index_json_dict.get('response', {}).get('docs', [])
    time_info = None
    for doc in docs:
        url_rec_list = doc.get('url', [])
        for url_rec in url_rec_list:
            url, mime_type, service_type = url_rec.split('|')
            if mime_type == 'application/netcdf' and service_type == 'HTTPServer':
                filename = doc.get('title', None)
                file_size = doc.get('size', -1)
                if not filename:
                    filename = os.path.basename(urllib.parse.urlparse(url)[2])
                if filename in file_list:
                    raise ValueError('filename %s already seen in dataset %s' % (filename, dataset_id))
                if not time_info:
                    time_info = _find_datetime_format(filename)
                # Start time will be extracted from filename using time_info
                start_time = None
                # We also reserve an end_time field, just in case.
                end_time = None
                if time_info:
                    time_format, p1, p2 = time_info
                    start_time = datetime.strptime(filename[p1:p2], time_format)
                    # Convert back to text, so we can JSON-encode it
                    start_time = datetime.strftime(start_time, _TIMESTAMP_FORMAT)
                file_list.append([filename, start_time, end_time, file_size, url])
                break

    def pick_start_time(file_info_rec):
        return file_info_rec[1]

    return sorted(file_list, key=pick_start_time)


class EsaCciOdpDataStore(DataStore):
    def __init__(self,
                 index_cache_used: bool = True,
                 index_cache_expiration_days: float = 1.0,
                 index_cache_json_dict: dict = None):
        super(EsaCciOdpDataStore, self).__init__()
        self._index_cache_used = index_cache_used
        self._index_cache_expiration_days = index_cache_expiration_days
        self._index_json_dict = index_cache_json_dict
        self._data_sources = []

    @property
    def index_cache_used(self):
        return self._index_cache_used

    @property
    def index_cache_expiration_days(self):
        return self._index_cache_expiration_days

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
        docs = self._index_json_dict.get('response', {}).get('docs', [])
        self._data_sources = []
        for doc in docs:
            self._data_sources.append(EsaCciOdpDataSource(self, doc))

    def _load_index(self) -> str:
        self._index_json_dict = _load_or_fetch_json(_fetch_solr_json,
                                                    fetch_json_args=[_ESGF_CEDA_URL, dict(type='Dataset',
                                                                                          replica='false',
                                                                                          latest='true',
                                                                                          project='esacci')],
                                                    cache_used=self._index_cache_used,
                                                    cache_dir=_DATA_ROOT,
                                                    cache_json_filename='dataset-list.json',
                                                    cache_timestamp_filename='dataset-list-timestamp.json',
                                                    cache_expiration_days=self._index_cache_expiration_days)


class EsaCciOdpDataSource(DataSource):
    def __init__(self,
                 data_store: EsaCciOdpDataStore,
                 json_dict: dict,
                 schema: Schema = None):
        super(EsaCciOdpDataSource, self).__init__()
        self._master_id = json_dict.get('master_id', None)
        self._dataset_id = json_dict.get('id', None)
        self._data_store = data_store
        self._json_dict = json_dict
        self._schema = schema
        self._file_list = None

    @property
    def name(self) -> str:
        return self._master_id

    @property
    def data_store(self) -> EsaCciOdpDataStore:
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
            parts = url_service.split('|')
            if len(parts) == 2:
                url, service = parts
                if service == desired_service:
                    return url
        return None

    def update_file_list(self) -> None:
        self._file_list = None
        self._init_file_list()

    def sync(self, time_range: TimeRange = (None, None), monitor: Monitor = Monitor.NULL) -> None:
        requested_start_date, requested_end_date = to_datetime(time_range[0]), to_datetime(time_range[1])

        self._init_file_list()
        if requested_start_date or requested_end_date:
            selected_file_list = []
            for file_rec in self._file_list:
                start_time = file_rec[1]
                ok = False
                if start_time:
                    if requested_start_date and requested_end_date:
                        ok = requested_start_date <= start_time <= requested_end_date
                    elif requested_start_date:
                        ok = requested_start_date <= start_time
                    elif requested_end_date:
                        ok = start_time <= requested_end_date
                if ok:
                    selected_file_list.append(file_rec)
        else:
            selected_file_list = self._file_list

        if not selected_file_list:
            print('No files found')

        with monitor.starting('Sync ' + self.name, len(selected_file_list)):
            dataset_dir = os.path.join(_DATA_ROOT, self._master_id)
            for filename, _, _, file_size, url in selected_file_list:
                dataset_file = os.path.join(dataset_dir, filename)
                # todo (forman, 20160915): must perform better checks on dataset_file if it is...
                # ... outdated or incomplete or corrupted.
                # JSON also includes "checksum" and "checksum_type" fields.
                if not os.path.isfile(dataset_file) or (file_size and os.path.getsize(dataset_file) != file_size):
                    sub_monitor = monitor.child(1.0)

                    def reporthook(block_number, read_size, total_file_size):
                        #print(block_number, read_size, total_file_size)
                        sub_monitor.progress(work=read_size)

                    with sub_monitor.starting(filename, file_size):
                        urllib.request.urlretrieve(url, filename=dataset_file, reporthook=reporthook)
                else:
                    monitor.progress(work=1.0)

    def open_dataset(self, time_range: TimeRange = (None, None)) -> xr.Dataset:
        self._init_file_list()
        raise NotImplementedError

    def _init_file_list(self) -> str:
        if self._file_list:
            return

        dataset_dir = os.path.join(_DATA_ROOT, self._master_id)
        file_list = _load_or_fetch_json(_fetch_file_list_json,
                                        fetch_json_args=[self._master_id, self._dataset_id],
                                        cache_used=self._data_store.index_cache_used,
                                        cache_dir=dataset_dir,
                                        cache_json_filename='file-list.json',
                                        cache_timestamp_filename='file-list-timestamp.txt',
                                        cache_expiration_days=self._data_store.index_cache_expiration_days)
        for file_rec in file_list:
            # Convert start_time string to datetime object
            file_rec[1] = datetime.strptime(file_rec[1], _TIMESTAMP_FORMAT)
        self._file_list = file_list

    def __str__(self):
        return self.name

    def _repr_html_(self):
        return self.info_string
