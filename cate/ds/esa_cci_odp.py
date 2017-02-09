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

__author__ = "Norman Fomferra (Brockmann Consult GmbH), " \
             "Marco Zühlke (Brockmann Consult GmbH), " \
             "Chris Bernat (Telespacio VEGA UK Inc.)"

"""
Description
===========

This plugin module adds the ESA CCI Open Data Portal's (ODP) ESGF service to
the data store registry.

Verification
============

The module's unit-tests are located in
`test/ds/test_esa_cci_odp.py <https://github.com/CCI-Tools/cate-core/blob/master/test/ds/test_esa_cci_ftp.py>`_
and may be executed using
``$ py.test test/ds/test_esa_cci_odp.py --cov=cate/ds/esa_cci_odp.py`` for extra code coverage information.

Components
==========
"""
import json
import os
import os.path
import re
import urllib.parse
import urllib.request
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Sequence, Tuple, Optional

import xarray as xr

from cate.core.ds import DATA_STORE_REGISTRY, DataStore, DataSource, Schema, open_xarray_dataset
from cate.core.ds import get_data_stores_path
from cate.util.monitor import Monitor

_ESGF_CEDA_URL = "https://esgf-index1.ceda.ac.uk/esg-search/search/"

_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

_RE_TO_DATETIME_FORMATS = patterns = [(re.compile(14 * '\\d'), '%Y%m%d%H%M%S'),
                                      (re.compile(12 * '\\d'), '%Y%m%d%H%M'),
                                      (re.compile(8 * '\\d'), '%Y%m%d'),
                                      (re.compile(6 * '\\d'), '%Y%m'),
                                      (re.compile(4 * '\\d'), '%Y')]

# days = 0, seconds = 0, microseconds = 0, milliseconds = 0, minutes = 0, hours = 0, weeks = 0
_TIME_FREQUENCY_TO_TIME_DELTA = dict([
    ('second', timedelta(seconds=1)),
    ('day', timedelta(days=1)),
    ('8-days', timedelta(days=8)),
    ('mon', timedelta(weeks=4)),
    ('yr', timedelta(days=365)),
])

_ODP_PROTOCOL_HTTP = 'HTTPServer'
_ODP_PROTOCOL_OPENDAP = 'OPENDAP'

_ODP_AVAILABLE_PROTOCOLS_LIST = [_ODP_PROTOCOL_HTTP, _ODP_PROTOCOL_OPENDAP]


def get_data_store_path():
    return os.environ.get('CATE_ESA_CCI_ODP_DATA_STORE_PATH',
                          os.path.join(get_data_stores_path(), 'esa_cci_odp'))


def set_default_data_store():
    """
    Defines the ESA CCI ODP data store and makes it the default data store.

    All data sources of the FTP data store are read from a JSON file ``esa_cci_ftp.json`` contained in this package.
    This JSON file has been generated from a scan of the entire FTP tree.
    """
    DATA_STORE_REGISTRY.add_data_store(EsaCciOdpDataStore())


def find_datetime_format(filename: str) -> Tuple[Optional[str], int, int]:
    for regex, time_format in _RE_TO_DATETIME_FORMATS:
        searcher = regex.search(filename)
        if searcher:
            p1, p2 = searcher.span()
            return time_format, p1, p2
    return None, -1, -1


def _fetch_solr_json(base_url, query_args, offset=0, limit=3500, timeout=10, monitor: Monitor=Monitor.NONE):
    """
    Return JSON value read from paginated Solr web-service.
    """
    combined_json_dict = None
    num_found = -1
    # we don't know ahead of time how much request are necessary
    with monitor.starting("Loading", 10):
        while True:
            monitor.progress(work=1)
            if monitor.is_cancelled():
                raise InterruptedError
            paging_query_args = dict(query_args or {})
            paging_query_args.update(offset=offset, limit=limit, format='application/solr+json')
            url = base_url + '?' + urllib.parse.urlencode(paging_query_args)
            with urllib.request.urlopen(url, timeout=timeout) as response:
                json_text = response.read()
                json_dict = json.loads(json_text.decode('utf-8'))
                if num_found is -1:
                    num_found = json_dict.get('response', {}).get('numFound', 0)
                if not combined_json_dict:
                    combined_json_dict = json_dict
                    if num_found < limit:
                        break
                else:
                    docs = json_dict.get('response', {}).get('docs', [])
                    combined_json_dict.get('response', {}).get('docs', []).extend(docs)
                    if num_found < offset + limit:
                        break
            offset += limit
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


def _fetch_file_list_json(dataset_id: str, dataset_query_id: str, monitor: Monitor=Monitor.NONE):
    file_index_json_dict = _fetch_solr_json(_ESGF_CEDA_URL,
                                            dict(type='File',
                                                 fields='url,title,size',
                                                 dataset_id=dataset_query_id,
                                                 replica='false',
                                                 latest='true',
                                                 project='esacci'),
                                            monitor=monitor)

    if not isinstance(file_index_json_dict, dict):
        return None

    file_list = []
    docs = file_index_json_dict.get('response', {}).get('docs', [])
    time_info = None
    for doc in docs:
        urls = dict()
        url_rec_list = doc.get('url', [])
        for url_rec in url_rec_list:
            url, mime_type, url_type = url_rec.split('|')
            urls[url_type] = url

        filename = doc.get('title', None)
        file_size = doc.get('size', -1)
        if not filename:
            filename = os.path.basename(urllib.parse.urlparse(url)[2])
        if filename in file_list:
            raise ValueError('filename {} already seen in dataset {}'
                             .format(filename, dataset_id))
        if not time_info:
            time_info = find_datetime_format(filename)
        # Start time will be extracted from filename using time_info
        start_time = None
        # We also reserve an end_time field, just in case.
        end_time = None
        if time_info:
            time_format, p1, p2 = time_info
            start_time = datetime.strptime(filename[p1:p2], time_format)
            # Convert back to text, so we can JSON-encode it
            start_time = datetime.strftime(start_time, _TIMESTAMP_FORMAT)
        file_list.append([filename, start_time, end_time, file_size, urls])

    def pick_start_time(file_info_rec):
        return file_info_rec[1]

    return sorted(file_list, key=pick_start_time)


class EsaCciOdpDataStore(DataStore):
    def __init__(self,
                 name: str = 'esa_cci_odp',
                 index_cache_used: bool = True,
                 index_cache_expiration_days: float = 1.0,
                 index_cache_json_dict: dict = None):
        super().__init__(name)
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

    def update_indices(self, update_file_lists: bool = False, monitor: Monitor = Monitor.NONE):
        with monitor.starting('Updating indices', 100):
            self._init_data_sources()
            monitor.progress(work=10 if update_file_lists else 100)
            if update_file_lists:
                child_monitor = monitor.child(work=90)
                with child_monitor.starting('Updating file lists', len(self._data_sources)):
                    for data_source in self._data_sources:
                        data_source.update_file_list()
                        child_monitor.progress(work=1)

    def query(self, name: str = None, monitor: Monitor = Monitor.NONE) -> Sequence['DataSource']:
        self._init_data_sources()
        if name:
            result = [data_source for data_source in self._data_sources if data_source.matches_filter(name)]
        else:
            result = self._data_sources
        return result

    def _repr_html_(self) -> str:
        self._init_data_sources()
        rows = []
        row_count = 0
        for data_source in self._data_sources:
            row_count += 1
            # noinspection PyProtectedMember
            rows.append('<tr><td><strong>%s</strong></td><td>%s</td></tr>' % (row_count, data_source._repr_html_()))
        return '<p>Contents of FileSetFileStore</p><table>%s</table>' % ('\n'.join(rows))

    def __repr__(self) -> str:
        return "EsaCciOdpDataStore"

    def _init_data_sources(self):
        if self._data_sources:
            return
        if self._index_json_dict is None:
            self._load_index()
        docs = self._index_json_dict.get('response', {}).get('docs', [])
        self._data_sources = []
        for doc in docs:
            self._data_sources.append(EsaCciOdpDataSource(self, doc))

    def _load_index(self):
        self._index_json_dict = _load_or_fetch_json(_fetch_solr_json,
                                                    fetch_json_args=[
                                                        _ESGF_CEDA_URL,
                                                        dict(type='Dataset',
                                                             replica='false',
                                                             latest='true',
                                                             project='esacci')],
                                                    cache_used=self._index_cache_used,
                                                    cache_dir=get_data_store_path(),
                                                    cache_json_filename='dataset-list.json',
                                                    cache_timestamp_filename='dataset-list-timestamp.json',
                                                    cache_expiration_days=self._index_cache_expiration_days)


INFO_FIELD_NAMES = sorted(["realization",
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
        self._temporal_coverage = None
        self._protocol_list = None

    @property
    def name(self) -> str:
        return self._master_id

    @property
    def data_store(self) -> EsaCciOdpDataStore:
        return self._data_store

    def temporal_coverage(self, monitor: Monitor=Monitor.NONE):
        if not self._temporal_coverage:
            self.update_file_list(monitor)
        return self._temporal_coverage

    @property
    def schema(self) -> Schema:
        return self._schema

    @property
    def meta_info(self) -> OrderedDict:
        # noinspection PyBroadException

        meta_info = OrderedDict()
        for name in INFO_FIELD_NAMES:
            value = self._json_dict.get(name, None)
            # Many values in the index JSON are one-element lists: turn them into scalars
            if isinstance(value, list) and len(value) == 1:
                value = value[0]
            meta_info[name] = value

        meta_info['protocols'] = self.protocols
        meta_info['variables'] = self._variables_list()

        return meta_info

    @property
    def cache_info(self) -> OrderedDict:
        coverage = OrderedDict()
        selected_file_list = self._find_files(None)
        if selected_file_list:
            dataset_dir = self.local_dataset_dir()
            for filename, date_from, date_to, none, none \
                    in selected_file_list:
                if os.path.exists(os.path.join(dataset_dir, filename)):
                    if date_from in coverage.values():
                        for temp_date_from, temp_date_to in coverage.items():
                            if temp_date_to == date_from:
                                coverage[temp_date_from] = date_to
                    elif date_to in coverage.keys():
                        temp_date_to = coverage[date_to]
                        coverage.pop(date_to)
                        coverage[date_from] = temp_date_to
                    else:
                        coverage[date_from] = date_to
        return coverage

    def _variables_list(self):
        variable_names = self._json_dict.get('variable', [])
        default_list = len(variable_names) * [None]
        units = self._json_dict.get('variable_units', default_list)
        long_names = self._json_dict.get('variable_long_name', default_list)
        standard_names = self._json_dict.get('cf_standard_name', default_list)

        variables_list = []
        for name, unit, long_name, standard_name in zip(variable_names, units, long_names, standard_names):
            variables_list.append(dict(name=name, units=unit, long_name=long_name, standard_name=standard_name))

        return variables_list

    @property
    def protocols(self) -> []:
        if self._protocol_list is None:
            self._protocol_list = [protocol for protocol in self._json_dict.get('access', [])
                                   if protocol in _ODP_AVAILABLE_PROTOCOLS_LIST]
        return self._protocol_list

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

    def update_file_list(self, monitor: Monitor=Monitor.NONE) -> None:
        self._file_list = None
        self._init_file_list(monitor)

    def sync(self,
             time_range: Tuple[datetime, datetime]=None,
             protocol: str=None,
             monitor: Monitor=Monitor.NONE) -> Tuple[int, int]:
        selected_file_list = self._find_files(time_range)
        if not selected_file_list:
            return 0, 0

        if protocol is None:
            protocol = _ODP_PROTOCOL_HTTP

        if protocol == _ODP_PROTOCOL_HTTP:
            dataset_dir = self.local_dataset_dir()

            # Find outdated files
            outdated_file_list = []
            for file_rec in selected_file_list:
                filename, _, _, file_size, url = file_rec
                dataset_file = os.path.join(dataset_dir, filename)
                # todo (forman, 20160915): must perform better checks on dataset_file if it is...
                # ... outdated or incomplete or corrupted.
                # JSON also includes "checksum" and "checksum_type" fields.
                if not os.path.isfile(dataset_file) or (file_size and os.path.getsize(dataset_file) != file_size):
                    outdated_file_list.append(file_rec)

            if not outdated_file_list:
                # No sync needed
                return 0, len(selected_file_list)

            with monitor.starting('Sync ' + self.name, len(outdated_file_list)):
                bytes_to_download = sum([file_rec[3] for file_rec in outdated_file_list])
                dl_stat = _DownloadStatistics(bytes_to_download)

                file_number = 1
                dataset_dir = self.local_dataset_dir()
                for filename, _, _, file_size, url in outdated_file_list:
                    if monitor.is_cancelled():
                        raise InterruptedError
                    dataset_file = os.path.join(dataset_dir, filename)
                    sub_monitor = monitor.child(work=1.0)

                    # noinspection PyUnusedLocal
                    def reporthook(block_number, read_size, total_file_size):
                        dl_stat.handle_chunk(read_size)
                        if monitor.is_cancelled():
                            raise InterruptedError
                        sub_monitor.progress(work=read_size, msg=str(dl_stat))

                    sub_monitor_msg = "file %d of %d" % (file_number, len(outdated_file_list))
                    with sub_monitor.starting(sub_monitor_msg, file_size):
                        urllib.request.urlretrieve(url[protocol], filename=dataset_file, reporthook=reporthook)
                    file_number += 1

            return len(outdated_file_list), len(selected_file_list)
        else:
            return 0, 0

    def delete_local(self, time_range: Tuple[datetime, datetime]) -> int:
        selected_file_list = self._find_files(time_range)
        if not selected_file_list:
            return 0

        dataset_dir = self.local_dataset_dir()
        removed_count = 0

        for filename, _, _, _, _ in selected_file_list:
            dataset_file = os.path.join(dataset_dir, filename)
            try:
                os.remove(dataset_file)
                removed_count += 1
            except:
                # File busy on Windows, move on
                pass

        return removed_count

    def local_dataset_dir(self):
        return os.path.join(get_data_store_path(), self._master_id)

    def _find_files(self, time_range):
        requested_start_date, requested_end_date = time_range if time_range else (None, None)
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
        return selected_file_list

    def open_dataset(self, time_range: Tuple[datetime, datetime]=None,
                     protocol: str=None) -> xr.Dataset:
        if protocol is None:
            protocol = _ODP_PROTOCOL_HTTP
        if protocol not in self.protocols:
            raise ValueError('Protocol \'{}\' is not supported.'
                             .format(protocol))

        selected_file_list = self._find_files(time_range)
        if not selected_file_list:
            msg = 'Data source \'{}\' does not seem to have any data files'.format(self.name)
            if time_range is not None:
                msg += ' in given time range {} to {}'.format(time_range[0], time_range[1])
            raise IOError(msg)

        files = []
        if protocol == _ODP_PROTOCOL_OPENDAP:
            files = [file_rec[4][protocol].replace('.html', '') for file_rec in selected_file_list]
        elif protocol == _ODP_PROTOCOL_HTTP:
            dataset_dir = self.local_dataset_dir()
            files = [os.path.join(dataset_dir, file_rec[0]) for file_rec in selected_file_list]
            for file in files:
                if not os.path.exists(file):
                    raise IOError('Missing local data files, consider synchronizing the dataset first.')

        try:
            return open_xarray_dataset(files)
        except OSError as e:
            raise IOError("Files: {} caused:\nOSError({}): {}".format(files, e.errno, e.strerror))

    def _init_file_list(self, monitor: Monitor=Monitor.NONE):
        if self._file_list:
            return

        file_list = _load_or_fetch_json(_fetch_file_list_json,
                                        fetch_json_args=[self._master_id, self._dataset_id],
                                        fetch_json_kwargs=dict(monitor=monitor),
                                        cache_used=self._data_store.index_cache_used,
                                        cache_dir=self.local_dataset_dir(),
                                        cache_json_filename='file-list.json',
                                        cache_timestamp_filename='file-list-timestamp.txt',
                                        cache_expiration_days=self._data_store.index_cache_expiration_days)

        time_frequency = self._json_dict.get('time_frequency', None)
        if time_frequency and isinstance(time_frequency, list):
            time_frequency = time_frequency[0]

        if time_frequency:
            time_delta = _TIME_FREQUENCY_TO_TIME_DELTA.get(time_frequency, timedelta(days=0))
        else:
            time_delta = timedelta(days=0)

        data_source_start_date = datetime(3000, 1, 1)
        data_source_end_date = datetime(1000, 1, 1)
        # Convert file_start_date from string to datetime object
        # Compute file_end_date from 'time_frequency' field
        # Compute the data source's temporal coverage
        for file_rec in file_list:
            file_start_date = datetime.strptime(file_rec[1], _TIMESTAMP_FORMAT)
            file_end_date = file_start_date + time_delta
            data_source_start_date = min(data_source_start_date, file_start_date)
            data_source_end_date = max(data_source_end_date, file_end_date)
            file_rec[1] = file_start_date
            file_rec[2] = file_end_date
        self._temporal_coverage = data_source_start_date, data_source_end_date
        self._file_list = file_list

    def __str__(self):
        return self.info_string

    def _repr_html_(self):
        return self.name

    def __repr__(self):
        return self.name


class _DownloadStatistics:
    def __init__(self, bytes_total):
        self.bytes_total = bytes_total
        self.bytes_done = 0
        self.startTime = datetime.now()

    def handle_chunk(self, bytes):
        self.bytes_done += bytes

    def asMB(self, bytes):
        return bytes / (1024 * 1024)

    def __str__(self):
        seconds = (datetime.now() - self.startTime).seconds
        if seconds > 0:
            mb_per_sec = self.asMB(self.bytes_done) / seconds
        else:
            mb_per_sec = 0
        return "%d of %d MB, speed %.3f MB/s" % \
               (self.asMB(self.bytes_done), self.asMB(self.bytes_total), mb_per_sec)
