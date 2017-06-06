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
import re
import urllib.parse
import urllib.request
import socket
import xarray as xr
from collections import OrderedDict
from datetime import datetime, timedelta
from math import ceil, floor, isnan
from typing import Sequence, Tuple, Optional, Any
from xarray.backends import NetCDF4DataStore

from owslib.csw import CatalogueServiceWeb
from owslib.namespaces import Namespaces

from cate.conf import get_config_value
from cate.conf.defaults import NETCDF_COMPRESSION_LEVEL
from cate.core.ds import DATA_STORE_REGISTRY, DataStore, DataSource, Schema, \
    open_xarray_dataset, get_data_stores_path, query_data_sources
from cate.core.types import PolygonLike, TimeRange, TimeRangeLike, VarNamesLike
from cate.ds.local import add_to_data_store_registry, LocalDataSource
from cate.util.monitor import Monitor

_ESGF_CEDA_URL = "https://esgf-index1.ceda.ac.uk/esg-search/search/"

_CSW_CEDA_URL = "http://csw1.cems.rl.ac.uk/geonetwork-CEDA/srv/eng/csw-CEDA-CCI"

_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

_REFERENCE_DATA_SOURCE_TYPE = "OPEN_DATA_PORTAL"

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

_CSW_TIMEOUT = 10
_CSW_MAX_RESULTS = 1000
_CSW_METADATA_CACHE_FILE = 'catalogue_metadata.xml'
_CSW_CACHE_FILE = 'catalogue.xml'

# by default there is no timeout
socket.setdefaulttimeout(10)


def get_data_store_path():
    return os.environ.get('CATE_LOCAL_DATA_STORE_PATH',
                          os.path.join(get_data_stores_path(), 'local'))


def get_metadata_store_path():
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


def _fetch_solr_json(base_url, query_args, offset=0, limit=3500, timeout=10, monitor: Monitor = Monitor.NONE):
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
    cache_json_file = None

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
        try:
            json_obj = fetch_json_function(*(fetch_json_args or []), **(fetch_json_kwargs or {}))
            if cache_used:
                os.makedirs(cache_dir, exist_ok=True)
                # noinspection PyUnboundLocalVariable
                with open(cache_json_file, 'w') as fp:
                    fp.write(json.dumps(json_obj, indent='  '))
                # noinspection PyUnboundLocalVariable
                with open(cache_timestamp_file, 'w') as fp:
                    fp.write(datetime.utcnow().strftime(_TIMESTAMP_FORMAT))
        except Exception as e:
            if cache_json_file and os.path.exists(cache_json_file):
                with open(cache_json_file) as fp:
                    json_text = fp.read()
                    json_obj = json.loads(json_text)
            else:
                raise e

    return json_obj


def _fetch_file_list_json(dataset_id: str, dataset_query_id: str, monitor: Monitor = Monitor.NONE):
    file_index_json_dict = _fetch_solr_json(_ESGF_CEDA_URL,
                                            dict(type='File',
                                                 fields='url,title,size',
                                                 dataset_id=dataset_query_id,
                                                 replica='false',
                                                 latest='True',
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
            filename = os.path.basename(urllib.parse.urlparse(urls[_ODP_PROTOCOL_HTTP])[2])
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
            if time_format:
                start_time = datetime.strptime(filename[p1:p2], time_format)
                # Convert back to text, so we can JSON-encode it
                start_time = datetime.strftime(start_time, _TIMESTAMP_FORMAT)
        file_list.append([filename, start_time, end_time, file_size, urls])

    def pick_start_time(file_info_rec):
        return file_info_rec[1] if file_info_rec[1] else datetime.max

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

        self._cci_catalogue_service = None
        self._cci_catalogue_data_dict = None

    @property
    def index_cache_used(self):
        return self._index_cache_used

    @property
    def index_cache_expiration_days(self):
        return self._index_cache_expiration_days

    @property
    def data_store_path(self) -> str:
        return get_metadata_store_path()

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

        if self._cci_catalogue_data_dict:
            for catalogue_data in self._cci_catalogue_data_dict.values():
                catalogue_item = catalogue_data.copy()
                catalogue_item.pop('data_sources')
                for ds_name in catalogue_data.get('data_sources'):
                    for idx, doc in enumerate(docs):
                        instance_id = doc.get('instance_id', None)
                        if ds_name == instance_id:
                            self._data_sources.append(EsaCciOdpDataSource(self, doc, catalogue_item))
                            del docs[idx]
                            break
        else:
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
                                                    cache_dir=get_metadata_store_path(),
                                                    cache_json_filename='dataset-list.json',
                                                    cache_timestamp_filename='dataset-list-timestamp.json',
                                                    cache_expiration_days=self._index_cache_expiration_days)

        if not self._cci_catalogue_service:
            self._cci_catalogue_service = EsaCciCatalogueService(_CSW_CEDA_URL)
        self._cci_catalogue_data_dict = _load_or_fetch_json(self._cci_catalogue_service.getrecords,
                                                            fetch_json_args=[],
                                                            cache_used=self._index_cache_used,
                                                            cache_dir=get_metadata_store_path(),
                                                            cache_json_filename='catalogue.json',
                                                            cache_timestamp_filename='catalogue-timestamp.json',
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
                           # catalogue data fields
                           "abstract",
                           "bbox_minx",
                           "bbox_miny",
                           "bbox_maxx",
                           "bbox_maxy",
                           "creation_date",
                           "publication_date",
                           "title",
                           "data_sources",
                           "licences",
                           "temporal_coverage_start",
                           "temporal_coverage_end"
                           ])


class EsaCciOdpDataSource(DataSource):
    def __init__(self,
                 data_store: EsaCciOdpDataStore,
                 json_dict: dict,
                 cci_catalogue_data: dict = None,
                 schema: Schema = None):
        super(EsaCciOdpDataSource, self).__init__()
        self._master_id = json_dict.get('master_id', None)
        self._dataset_id = json_dict.get('id', None)
        self._instance_id = json_dict.get('instance_id', None)

        if json_dict.get('xlink', None):
            self._uuid = json_dict.get('xlink')[0].split('|', 1)[0].rsplit('/', 1)[1]
        else:
            self._uuid = None

        self._data_store = data_store
        self._json_dict = json_dict
        self._schema = schema
        self._catalogue_data = cci_catalogue_data

        self._file_list = None

        self._temporal_coverage = None
        self._protocol_list = None
        self._meta_info = None

    @property
    def name(self) -> str:
        return self._master_id

    @property
    def data_store(self) -> EsaCciOdpDataStore:
        return self._data_store

    @property
    def spatial_coverage(self) -> Optional[PolygonLike]:
        if self._catalogue_data \
                and self._catalogue_data.get('bbox_minx', None) and self._catalogue_data.get('bbox_miny', None) \
                and self._catalogue_data.get('bbox_maxx', None) and self._catalogue_data.get('bbox_maxy', None):

            return PolygonLike.convert([
                self._catalogue_data.get('bbox_minx'),
                self._catalogue_data.get('bbox_miny'),
                self._catalogue_data.get('bbox_maxx'),
                self._catalogue_data.get('bbox_maxy')
            ])
        return None

    def temporal_coverage(self, monitor: Monitor = Monitor.NONE) -> Optional[TimeRange]:
        if not self._temporal_coverage:
            temp_coverage_start = self._catalogue_data.get('temporal_coverage_start', None)
            temp_coverage_end = self._catalogue_data.get('temporal_coverage_end', None)
            if temp_coverage_start and temp_coverage_end:
                self._temporal_coverage = TimeRangeLike.convert("{},{}".format(temp_coverage_start, temp_coverage_end))
            else:
                self.update_file_list(monitor)
        if self._temporal_coverage:
            return self._temporal_coverage
        return None

    @property
    def schema(self) -> Schema:
        return self._schema

    @property
    def meta_info(self) -> OrderedDict:
        # noinspection PyBroadException
        if not self._meta_info:
            meta_info = OrderedDict()
            for name in INFO_FIELD_NAMES:
                value = self._json_dict.get(name, None)
                # Many values in the index JSON are one-element lists: turn them into scalars
                if isinstance(value, list) and len(value) == 1:
                    value = value[0]
                meta_info[name] = value

            meta_info['protocols'] = self.protocols
            meta_info['variables'] = self._variables_list()

            if self._catalogue_data:
                meta_info.update(self._catalogue_data)

            self._meta_info = meta_info

        return self._meta_info

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

    def find_url(self, desired_service='HTTP'):
        for url_service in self._json_dict.get('url', []):
            parts = url_service.split('|')
            if len(parts) == 2:
                url, service = parts
                if service == desired_service:
                    return url
        return None

    def update_file_list(self, monitor: Monitor = Monitor.NONE) -> None:
        self._file_list = None
        self._init_file_list(monitor)

    def sync(self,
             time_range: TimeRangeLike.TYPE = None,
             protocol: str = None,
             monitor: Monitor = Monitor.NONE) -> Tuple[int, int]:

        if protocol == _ODP_PROTOCOL_HTTP:
            self.make_local(self._master_id(), None, time_range, None, None, monitor)
        else:
            raise ValueError('Unsupported protocol', protocol)
        return 0, 0

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

    def delete_local(self, time_range: TimeRangeLike.TYPE) -> int:

        if time_range[0] >= self._temporal_coverage[0] \
                and time_range[1] <= self._temporal_coverage[1]:
            if time_range[0] == self._temporal_coverage[0] \
                    or time_range[1] == self._temporal_coverage[1]:
                return self.update_local(self._master_id, time_range)
        return 0

    def local_dataset_dir(self):
        return os.path.join(get_data_store_path(), self._master_id)

    def local_metadata_dataset_dir(self):
        return os.path.join(get_metadata_store_path(), self._master_id)

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

    def open_dataset(self,
                     time_range: TimeRangeLike.TYPE = None,
                     region: PolygonLike.TYPE = None,
                     var_names: VarNamesLike.TYPE = None,
                     protocol: str = None) -> Any:
        time_range = TimeRangeLike.convert(time_range) if time_range else None
        region = PolygonLike.convert(region) if region else None
        var_names = VarNamesLike.convert(var_names) if var_names else None

        selected_file_list = self._find_files(time_range)
        if not selected_file_list:
            msg = 'Data source \'{}\' does not seem to have any data files'.format(self.name)
            if time_range is not None:
                msg += ' in given time range {}'.format(TimeRangeLike.format(time_range))
            raise IOError(msg)

        files = self._get_urls_list(selected_file_list, _ODP_PROTOCOL_OPENDAP)
        try:
            ds = open_xarray_dataset(files)
            if region:
                [lon_min, lat_min, lon_max, lat_max] = region.bounds
                ds = ds.sel(drop=False, lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max))
            if var_names:
                ds = ds.drop([var_name for var_name in ds.variables.keys() if var_name not in var_names])
            return ds

        except OSError as e:
            raise IOError("Files: {} caused:\nOSError({}): {}".format(files, e.errno, e.strerror))

    @staticmethod
    def _get_urls_list(files_description_list, protocol) -> Sequence[str]:
        """
        Returns urls list extracted from reference esgf specific files description json list
        :param files_description_list:
        :param protocol:
        :return:
        """
        return [file_rec[4][protocol].replace('.html', '') for file_rec in files_description_list]

    @staticmethod
    def _get_harmonized_coordinate_value(attrs: dict, attr_name: str):
        value = attrs.get(attr_name, 'nan')
        if isinstance(value, str):
            return float(value.rstrip('degrees').rstrip('f'))
        return value

    def _make_local(self,
                    local_ds: LocalDataSource,
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

        if region or var_names:
            protocol = _ODP_PROTOCOL_OPENDAP
        else:
            protocol = _ODP_PROTOCOL_HTTP

        local_path = os.path.join(local_ds.data_store.data_store_path, local_id)
        if not os.path.exists(local_path):
            os.makedirs(local_path)

        selected_file_list = self._find_files(time_range)

        if protocol == _ODP_PROTOCOL_OPENDAP:

            files = self._get_urls_list(selected_file_list, protocol)
            monitor.start('Sync ' + self.name, total_work=len(files))
            for idx, dataset_uri in enumerate(files):
                child_monitor = monitor.child(work=1)

                file_name = os.path.basename(dataset_uri)
                local_filepath = os.path.join(local_path, file_name)

                time_coverage_start = selected_file_list[idx][1]
                time_coverage_end = selected_file_list[idx][2]

                remote_netcdf = None
                local_netcdf = None
                try:
                    remote_netcdf = NetCDF4DataStore(dataset_uri)

                    local_netcdf = NetCDF4DataStore(local_filepath, mode='w', persist=True)
                    local_netcdf.set_attributes(remote_netcdf.get_attrs())

                    remote_dataset = xr.Dataset.load_store(remote_netcdf)

                    process_region = False
                    if region:
                        geo_lat_min = self._get_harmonized_coordinate_value(remote_dataset.attrs, 'geospatial_lat_min')
                        geo_lat_max = self._get_harmonized_coordinate_value(remote_dataset.attrs, 'geospatial_lat_max')
                        geo_lon_min = self._get_harmonized_coordinate_value(remote_dataset.attrs, 'geospatial_lon_min')
                        geo_lon_max = self._get_harmonized_coordinate_value(remote_dataset.attrs, 'geospatial_lon_max')

                        geo_lat_res = self._get_harmonized_coordinate_value(remote_dataset.attrs,
                                                                            'geospatial_lon_resolution')
                        geo_lon_res = self._get_harmonized_coordinate_value(remote_dataset.attrs,
                                                                            'geospatial_lat_resolution')
                        if not (isnan(geo_lat_min) or isnan(geo_lat_max) or
                                isnan(geo_lon_min) or isnan(geo_lon_max) or
                                isnan(geo_lat_res) or isnan(geo_lon_res)):
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
                            [var_name for var_name in remote_dataset.variables.keys() if var_name != sel_var_name])
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
                        local_ds.add_dataset(os.path.join(local_id, file_name),
                                             (time_coverage_start, time_coverage_end))

                child_monitor.done()
        else:
            outdated_file_list = []
            for file_rec in selected_file_list:
                filename, _, _, file_size, url = file_rec
                dataset_file = os.path.join(local_path, filename)
                # todo (forman, 20160915): must perform better checks on dataset_file if it is...
                # ... outdated or incomplete or corrupted.
                # JSON also includes "checksum" and "checksum_type" fields.
                if not os.path.isfile(dataset_file) or (file_size and os.path.getsize(dataset_file) != file_size):
                    outdated_file_list.append(file_rec)

            if outdated_file_list:
                with monitor.starting('Sync ' + self.name, len(outdated_file_list)):
                    bytes_to_download = sum([file_rec[3] for file_rec in outdated_file_list])
                    dl_stat = _DownloadStatistics(bytes_to_download)

                    file_number = 1

                    for filename, coverage_from, coverage_to, file_size, url in outdated_file_list:
                        if monitor.is_cancelled():
                            raise InterruptedError
                        dataset_file = os.path.join(local_path, filename)
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
                        local_ds.add_dataset(os.path.join(local_id, filename), (coverage_from, coverage_to))
        local_ds.save()
        monitor.done()

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

    def _init_file_list(self, monitor: Monitor = Monitor.NONE):
        if self._file_list:
            return

        file_list = _load_or_fetch_json(_fetch_file_list_json,
                                        fetch_json_args=[self._master_id, self._dataset_id],
                                        fetch_json_kwargs=dict(monitor=monitor),
                                        cache_used=self._data_store.index_cache_used,
                                        cache_dir=self.local_metadata_dataset_dir(),
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
            if file_rec[1]:
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

    def handle_chunk(self, chunk):
        self.bytes_done += chunk

    @staticmethod
    def _to_mibs(bytes_count):
        return bytes_count / (1024 * 1024)

    def __str__(self):
        seconds = (datetime.now() - self.startTime).seconds
        if seconds > 0:
            mb_per_sec = self._to_mibs(self.bytes_done) / seconds
        else:
            mb_per_sec = 0
        return "%d of %d MiB @ %.3f MiB/s" % \
               (self._to_mibs(self.bytes_done), self._to_mibs(self.bytes_total), mb_per_sec)


class EsaCciCatalogueService:

    def __init__(self, catalogue_url: str):

        self._catalogue_url = catalogue_url

        self._catalogue = None
        self._catalogue_service = None

        self._namespaces = Namespaces()

    def reset(self):
        self._catalogue = None
        self._catalogue_service = None

    def getrecords(self, monitor: Monitor = Monitor.NONE):
        if not self._catalogue_service:
            self._init_service()

        if not self._catalogue:
            self._build_catalogue(monitor.child(1))

        return self._catalogue

    def _build_catalogue(self, monitor: Monitor = Monitor.NONE):

        self._catalogue = {}

        catalogue_metadata = {}

        start_position = 0
        max_records = _CSW_MAX_RESULTS

        matches = -1
        while True:
            # fetch record metadata
            self._catalogue_service.getrecords2(esn='full', outputschema=self._namespaces.get_namespace('gmd'),
                                                startposition=start_position, maxrecords=max_records)
            if matches == -1:
                # set counters, start progress monitor
                matches = self._catalogue_service.results.get('matches')
                if matches == 0:
                    break
                monitor.start(label="Fetching catalogue data... (%d records)" % matches,
                              total_work=ceil(matches / max_records))

            catalogue_metadata.update(self._catalogue_service.records)
            monitor.progress(work=1)

            # bump counters
            start_position += max_records
            if start_position > matches:
                break

        self._catalogue = {
            record.identification.uricode[0]: {
                    'abstract': record.identification.abstract,
                    'bbox_minx': record.identification.bbox.minx if record.identification.bbox else None,
                    'bbox_miny': record.identification.bbox.miny if record.identification.bbox else None,
                    'bbox_maxx': record.identification.bbox.maxx if record.identification.bbox else None,
                    'bbox_maxy': record.identification.bbox.maxy if record.identification.bbox else None,
                    'creation_date':
                    next(iter(e.date for e in record.identification.date if e and e.type == 'creation'), None),
                    'publication_date':
                    next(iter(e.date for e in record.identification.date if e and e.type == 'publication'), None),
                    'title': record.identification.title,
                    'data_sources': record.identification.uricode[1:],
                    'licences': record.identification.uselimitation,
                    'temporal_coverage_start': record.identification.temporalextent_start,
                    'temporal_coverage_end': record.identification.temporalextent_end
            }
            for record in catalogue_metadata.values()
            if record.identification and len(record.identification.uricode) > 0
        }
        monitor.done()

    def _init_service(self):
        if self._catalogue:
            return
        if not self._catalogue_service:
            self._catalogue_service = CatalogueServiceWeb(url=self._catalogue_url, timeout=_CSW_TIMEOUT, skip_caps=True)
