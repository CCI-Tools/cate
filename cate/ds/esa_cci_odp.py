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

"""
Description
===========

This plugin module adds the ESA CCI Open Data Portal's (ODP) ESGF service to
the data store registry.

Verification
============

The module's unit-tests are located in
`test/ds/test_esa_cci_odp.py <https://github.com/CCI-Tools/cate/blob/master/test/ds/test_esa_cci_ftp.py>`_
and may be executed using
``$ py.test test/ds/test_esa_cci_odp.py --cov=cate/ds/esa_cci_odp.py`` for extra code coverage information.

Components
==========
"""
import json
import os
import re
import socket
import ssl
import urllib.parse
import urllib.request
from collections import OrderedDict
from datetime import datetime, timedelta
from math import ceil
from typing import Sequence, Tuple, Optional, Any, Dict
from urllib.error import URLError, HTTPError

import pandas as pd
import xarray as xr
from owslib.csw import CatalogueServiceWeb
from owslib.namespaces import Namespaces

from cate.conf import get_config_value, get_data_stores_path
from cate.conf.defaults import NETCDF_COMPRESSION_LEVEL
from cate.core.ds import DATA_STORE_REGISTRY, DataAccessError, NetworkError, DataStore, DataSource, Schema, \
    open_xarray_dataset
from cate.core.opimpl import subset_spatial_impl, normalize_impl, adjust_spatial_attrs_impl
from cate.core.types import PolygonLike, TimeLike, TimeRange, TimeRangeLike, VarNamesLike
from cate.ds.local import add_to_data_store_registry, LocalDataSource, LocalDataStore
from cate.util.monitor import Cancellation, Monitor

ESA_CCI_ODP_DATA_STORE_ID = 'esa_cci_odp'

__author__ = "Norman Fomferra (Brockmann Consult GmbH), " \
             "Marco Zühlke (Brockmann Consult GmbH), " \
             "Chris Bernat (Telespazio VEGA UK Ltd), " \
             "Paolo Pesciullesi (Telespazio VEGA UK Ltd)"

_ESGF_CEDA_URL = "https://cci-odp-index.ceda.ac.uk/esg-search/search/"
_CSW_CEDA_URL = "https://csw-cci.ceda.ac.uk/geonetwork/srv/eng/csw-CEDA-CCI"

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

_YEAR_REALIZATION = re.compile(4 * '\\d')

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
                          os.path.join(get_data_stores_path(), ESA_CCI_ODP_DATA_STORE_ID))


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


def get_exclude_variables_fix_known_issues(ds_id: str) -> [str]:
    """
    This method applies fixes to the parameters of a 'make_local' invocation.
    """
    if ds_id:
        # the 't0' variable in these SOILMOISTURE data sources
        # can not be decoded by xarray and lead to an unusable dataset
        # see: https://github.com/CCI-Tools/cate/issues/326
        soil_moisture_datasets = [
            'esacci.SOILMOISTURE.day.L3S.SSMS.multi-sensor.multi-platform.ACTIVE.03-2.r1',
            'esacci.SOILMOISTURE.day.L3S.SSMV.multi-sensor.multi-platform.COMBINED.03-2.r1',
            'esacci.SOILMOISTURE.day.L3S.SSMV.multi-sensor.multi-platform.PASSIVE.03-2.r1'
        ]
        if ds_id in soil_moisture_datasets:
            return [{'name': 't0', 'comment': "can not be decoded by xarray and lead to an unusable dataset\n"
                                              "see: https://github.com/CCI-Tools/cate/issues/326"}]
        return []


def _fetch_solr_json(base_url, query_args, offset=0, limit=3500, timeout=10, monitor: Monitor = Monitor.NONE):
    """
    Return JSON value read from paginated Solr web-service.
    """
    combined_json_dict = None
    num_found = -1
    # we don't know ahead of time how many requests are necessary
    with monitor.starting("Loading", 10):
        while True:
            monitor.progress(work=1)
            paging_query_args = dict(query_args or {})
            # noinspection PyArgumentList
            paging_query_args.update(offset=offset, limit=limit, format='application/solr+json')
            url = base_url + '?' + urllib.parse.urlencode(paging_query_args)
            error_message = f"Failed accessing CCI ODP service {base_url}"
            try:
                # noinspection PyProtectedMember
                context = ssl._create_unverified_context()
                with urllib.request.urlopen(url, timeout=timeout, context=context) as response:
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
            except urllib.error.HTTPError as e:
                raise DataAccessError(f"{error_message}: {e}") from e
            except (urllib.error.URLError, socket.timeout) as e:
                raise NetworkError(f"{error_message}: {e}") from e
            except OSError as e:
                raise DataAccessError(f"{error_message}: {e}") from e
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
    # noinspection PyShadowingBuiltins
    def __init__(self,
                 id: str = 'esa_cci_odp',
                 title: str = 'ESA CCI Open Data Portal',
                 index_cache_used: bool = True,
                 index_cache_expiration_days: float = 1.0,
                 index_cache_json_dict: dict = None,
                 index_cache_update_tag: str = None):
        super().__init__(id, title=title, is_local=False)
        self._index_cache_used = index_cache_used
        self._index_cache_expiration_days = index_cache_expiration_days
        self._esgf_data = index_cache_json_dict
        self._index_cache_update_tag = index_cache_update_tag
        self._data_sources = []

        self._csw_data = None

    @property
    def index_cache_used(self):
        return self._index_cache_used

    @property
    def index_cache_expiration_days(self):
        return self._index_cache_expiration_days

    @property
    def data_store_path(self) -> str:
        return get_metadata_store_path()

    def query(self, ds_id: str = None, query_expr: str = None, monitor: Monitor = Monitor.NONE) \
            -> Sequence['DataSource']:
        self._init_data_sources()
        if ds_id or query_expr:
            return [ds for ds in self._data_sources if ds.matches(ds_id=ds_id, query_expr=query_expr)]
        return self._data_sources

    def get_updates(self, reset=False) -> Dict:
        """
        Ask to retrieve the differences found between a previous
        dataStore status and the current one,
        The implementation return a dictionary with the new ['new'] and removed ['del'] dataset.
        it also return the reference time to the datastore status taken as previous snapshot,
        Reset flag is used to clean up the support files, freeze and diff.
        :param: reset=False. Set this flag to true to clean up all the support files forcing a
                synchronization with the remote catalog
        :return: A dictionary with keys { 'generated', 'source_ref_time', 'new', 'del' }.
                 genetated: generation time, when the check has been executed
                 source_ref_time: when the local copy of the remoted dataset hes been made.
                                  It is also used by the system to refresh the current images when
                                  is older then 1 day.
                 new: a list of new dataset entry
                 del: a list of removed dataset
        """
        diff_file = os.path.join(get_metadata_store_path(), self._get_update_tag() + '-diff.json')

        if os.path.isfile(diff_file):
            with open(diff_file, 'r') as json_in:
                report = json.load(json_in)
        else:
            generated = datetime.now()
            report = {"generated": str(generated),
                      "source_ref_time": str(generated),
                      "new": list(),
                      "del": list()}

            # clean up when requested
        if reset:
            if os.path.isfile(diff_file):
                os.remove(diff_file)
            frozen_file = os.path.join(get_metadata_store_path(), self._get_update_tag() + '-freeze.json')
            if os.path.isfile(frozen_file):
                os.remove(frozen_file)
        return report

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
        return "EsaCciOdpDataStore (%s)" % self.id

    def _init_data_sources(self):
        os.makedirs(get_metadata_store_path(), exist_ok=True)
        if self._data_sources:
            return
        if self._esgf_data is None:
            self._load_index()
        if self._esgf_data is None:
            return

        docs = self._esgf_data.get('response', {}).get('docs', [])
        data_sources = []
        if self._csw_data:
            for catalogue_data in self._csw_data.values():
                catalogue_item = catalogue_data.copy()
                catalogue_item.pop('data_sources')
                for ds_name in catalogue_data.get('data_sources'):
                    for idx, doc in enumerate(docs):
                        instance_id = doc.get('instance_id', None)
                        if ds_name == instance_id:
                            data_sources.append(EsaCciOdpDataSource(self, doc, catalogue_item))
                            del docs[idx]
                            break
        else:
            for doc in docs:
                data_sources.append(EsaCciOdpDataSource(self, doc))
        self._data_sources = data_sources
        self._check_source_diff()
        self._freeze_source()

    def _get_update_tag(self):
        """
        return the name to be used and TAG to check and freee the dataset list
        A datastore could be created with a json file so to avoid collision with a
        previous frozen DS the user can set a TAG
        :return:
        """
        if self._index_cache_update_tag:
            return self._index_cache_update_tag
        return 'dataset-list'

    def _check_source_diff(self):
        """
        This routine is responsible to find differences (new dataset or removed dataset)
        between the most updated list and the provious frozen one.
        It generate a file dataset-list-diff.json with the results.
        generated time: time of report generation
        source_ref_time : time of the frozen (previous) dataset list
        new: list of item founded as new
        del: list of items found as removed

        It use a persistent output to keep trace of the previous changes in case the
        frozen dataset is updated too.
        :return:
        """
        frozen_file = os.path.join(get_metadata_store_path(), self._get_update_tag() + '-freeze.json')
        diff_file = os.path.join(get_metadata_store_path(), self._get_update_tag() + '-diff.json')
        deleted = []
        added = []

        if os.path.isfile(frozen_file):
            with open(frozen_file, 'r') as json_in:
                frozen_source = json.load(json_in)

            ds_new = set([ds.to_json()['id'] for ds in self._data_sources])
            ds_old = set([ds for ds in frozen_source['data']])
            for ds in (ds_old - ds_new):
                deleted.append(ds)

            for ds in (ds_new - ds_old):
                added.append(ds)

            if deleted or added:
                generated = datetime.now()
                diff_source = {'generated': str(generated),
                               'source_ref_time': frozen_source['source_ref_time'],
                               'new': added,
                               'del': deleted}
                with open(diff_file, 'w+') as json_out:
                    json.dump(diff_source, json_out)

    def _freeze_source(self):
        """
        Freeze a dataset list when needed.
        The file generated by this method is a snapshop of the dataset list with an expiration time of one day.
        It is updated to the current when is expired, the file is used to compare the
        previous dataset status with the current in order to find differences.

        the frozen file is saved in the data store meta directory with the nane
        'dataset-list-freeze.json'
        :return:
        """
        frozen_file = os.path.join(get_metadata_store_path(), self._get_update_tag() + '-freeze.json')
        save_it = True
        now = datetime.now()
        if os.path.isfile(frozen_file):
            with open(frozen_file, 'r') as json_in:
                freezed_source = json.load(json_in)
            source_ref_time = pd.to_datetime(freezed_source['source_ref_time'])
            save_it = (now > source_ref_time + timedelta(days=1))

        if save_it:
            data = [ds.to_json()['id'] for ds in self._data_sources]
            freezed_source = {'source_ref_time': str(now),
                              'data': data}
            with open(frozen_file, 'w+') as json_out:
                json.dump(freezed_source, json_out)

    def _load_index(self):
        esgf_json_dict = _load_or_fetch_json(_fetch_solr_json,
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

        cci_catalogue_service = EsaCciCatalogueService(_CSW_CEDA_URL)
        csw_json_dict = _load_or_fetch_json(cci_catalogue_service.getrecords,
                                            fetch_json_args=[],
                                            cache_used=self._index_cache_used,
                                            cache_dir=get_metadata_store_path(),
                                            cache_json_filename='catalogue.json',
                                            cache_timestamp_filename='catalogue-timestamp.json',
                                            cache_expiration_days=self._index_cache_expiration_days)

        self._csw_data = csw_json_dict
        self._esgf_data = esgf_json_dict


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
    def id(self) -> str:
        return self._master_id

    @property
    def uuid(self) -> Optional[str]:
        return self._uuid

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
                # ODP datasets that are split into per-year datasets
                # have the year they are covering in the 'realization' attribute
                # the CSW does not have separate temporal coverages for them
                realization = self._json_dict.get('realization', None)
                if realization and len(realization):
                    matcher = _YEAR_REALIZATION.match(realization[0])
                    if matcher:
                        year = matcher.group(0)
                        rel_start = max(self._temporal_coverage[0], datetime(int(year), 1, 1))
                        rel_end = min(self._temporal_coverage[1], datetime(int(year) + 1, 1, 1) - timedelta(seconds=1))
                        self._temporal_coverage = (rel_start, rel_end)
            else:
                self.update_file_list(monitor)
        if self._temporal_coverage:
            return self._temporal_coverage
        return None

    @property
    def variables_info(self):
        return self._variables_list()

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
            meta_info['uuid'] = self._uuid

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
                     protocol: str = None,
                     monitor: Monitor = Monitor.NONE) -> Any:
        time_range = TimeRangeLike.convert(time_range) if time_range else None
        var_names = VarNamesLike.convert(var_names) if var_names else None

        selected_file_list = self._find_files(time_range)
        if not selected_file_list:
            raise self._empty_error(time_range)

        files = self._get_urls_list(selected_file_list, _ODP_PROTOCOL_OPENDAP)
        try:
            return open_xarray_dataset(files, region=region, var_names=var_names, monitor=monitor)
        except HTTPError as e:
            raise self._cannot_access_error(time_range, region, var_names,
                                            cause=e) from e
        except (URLError, socket.timeout) as e:
            raise self._cannot_access_error(time_range, region, var_names,
                                            cause=e, error_cls=NetworkError) from e
        except OSError as e:
            raise self._cannot_access_error(time_range, region, var_names,
                                            cause=e) from e

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

        local_id = local_ds.id
        time_range = TimeRangeLike.convert(time_range)
        var_names = VarNamesLike.convert(var_names)

        excluded_variables = get_exclude_variables_fix_known_issues(self.id)

        compression_level = get_config_value('NETCDF_COMPRESSION_LEVEL', NETCDF_COMPRESSION_LEVEL)
        compression_enabled = True if compression_level > 0 else False

        do_update_of_verified_time_coverage_start_once = True
        verified_time_coverage_start = None
        verified_time_coverage_end = None

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
        if not selected_file_list:
            raise self._empty_error(time_range)

        try:
            if protocol == _ODP_PROTOCOL_OPENDAP:
                do_update_of_variables_meta_info_once = True
                do_update_of_region_meta_info_once = True

                files = self._get_urls_list(selected_file_list, protocol)
                with monitor.starting('Sync ' + self.id, total_work=len(files)):
                    for idx, dataset_uri in enumerate(files):
                        child_monitor = monitor.child(work=1)

                        file_name = os.path.basename(dataset_uri)
                        local_filepath = os.path.join(local_path, file_name)

                        time_coverage_start = selected_file_list[idx][1]
                        time_coverage_end = selected_file_list[idx][2]

                        with child_monitor.starting(label=file_name, total_work=100):
                            remote_dataset = xr.open_dataset(dataset_uri,
                                                             drop_variables=[variable.get('name') for variable in
                                                                             excluded_variables])
                            remote_dataset_root = remote_dataset
                            child_monitor.progress(work=20)

                            if var_names:
                                remote_dataset = remote_dataset.drop([var_name for var_name in remote_dataset.data_vars.keys()
                                                                      if var_name not in var_names])
                            if region:
                                remote_dataset = normalize_impl(remote_dataset)
                                remote_dataset = subset_spatial_impl(remote_dataset, region)
                                remote_dataset = adjust_spatial_attrs_impl(remote_dataset, allow_point=False)
                                if do_update_of_region_meta_info_once:
                                    local_ds.meta_info['bbox_minx'] = remote_dataset.attrs['geospatial_lon_min']
                                    local_ds.meta_info['bbox_maxx'] = remote_dataset.attrs['geospatial_lon_max']
                                    local_ds.meta_info['bbox_maxy'] = remote_dataset.attrs['geospatial_lat_max']
                                    local_ds.meta_info['bbox_miny'] = remote_dataset.attrs['geospatial_lat_min']
                                    do_update_of_region_meta_info_once = False
                            if compression_enabled:
                                for sel_var_name in remote_dataset.variables.keys():
                                    remote_dataset.variables.get(sel_var_name).encoding.update(encoding_update)
                            # Note: we are using engine='h5netcdf' here because the default engine='netcdf4'
                            # causes crashes in file "netCDF4/_netCDF4.pyx" with currently used netcdf4-1.4.2 conda
                            # package from conda-forge. This occurs whenever remote_dataset.to_netcdf() is called a
                            # second time in this loop.
                            # Probably related to https://github.com/pydata/xarray/issues/2560.
                            # And probably fixes Cate issues #823, #822, #818, #816, #783.
                            remote_dataset.to_netcdf(local_filepath, format='NETCDF4', engine='h5netcdf')
                            child_monitor.progress(work=75)

                            if do_update_of_variables_meta_info_once:
                                variables_info = local_ds.meta_info.get('variables', [])
                                local_ds.meta_info['variables'] = [var_info for var_info in variables_info
                                                                   if var_info.get('name')
                                                                   in remote_dataset.variables.keys()
                                                                   and var_info.get('name')
                                                                   not in remote_dataset.dims.keys()]
                                do_update_of_variables_meta_info_once = False
                            local_ds.add_dataset(os.path.join(local_id, file_name),
                                                 (time_coverage_start, time_coverage_end))
                            if do_update_of_verified_time_coverage_start_once:
                                verified_time_coverage_start = time_coverage_start
                                do_update_of_verified_time_coverage_start_once = False
                            verified_time_coverage_end = time_coverage_end
                            child_monitor.progress(work=5)

                            remote_dataset_root.close()
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
                    with monitor.starting('Sync ' + self.id, len(outdated_file_list)):
                        bytes_to_download = sum([file_rec[3] for file_rec in outdated_file_list])
                        dl_stat = _DownloadStatistics(bytes_to_download)

                        file_number = 1

                        for filename, coverage_from, coverage_to, file_size, url in outdated_file_list:
                            dataset_file = os.path.join(local_path, filename)
                            child_monitor = monitor.child(work=1.0)

                            # noinspection PyUnusedLocal
                            def reporthook(block_number, read_size, total_file_size):
                                dl_stat.handle_chunk(read_size)
                                child_monitor.progress(work=read_size, msg=str(dl_stat))

                            sub_monitor_msg = "file %d of %d" % (file_number, len(outdated_file_list))
                            with child_monitor.starting(sub_monitor_msg, file_size):
                                urllib.request.urlretrieve(url[protocol], filename=dataset_file, reporthook=reporthook)
                            file_number += 1
                            local_ds.add_dataset(os.path.join(local_id, filename), (coverage_from, coverage_to))

                            if do_update_of_verified_time_coverage_start_once:
                                verified_time_coverage_start = coverage_from
                                do_update_of_verified_time_coverage_start_once = False
                            verified_time_coverage_end = coverage_to
        except HTTPError as e:
            raise self._cannot_access_error(time_range, region, var_names,
                                            verb="synchronize", cause=e) from e
        except (URLError, socket.timeout) as e:
            raise self._cannot_access_error(time_range, region, var_names,
                                            verb="synchronize", cause=e,
                                            error_cls=NetworkError) from e
        except OSError as e:
            raise self._cannot_access_error(time_range, region, var_names,
                                            verb="synchronize", cause=e) from e

        local_ds.meta_info['temporal_coverage_start'] = TimeLike.format(verified_time_coverage_start)
        local_ds.meta_info['temporal_coverage_end'] = TimeLike.format(verified_time_coverage_end)
        local_ds.meta_info['exclude_variables'] = excluded_variables
        local_ds.save(True)

    def make_local(self,
                   local_name: str,
                   local_id: str = None,
                   time_range: TimeRangeLike.TYPE = None,
                   region: PolygonLike.TYPE = None,
                   var_names: VarNamesLike.TYPE = None,
                   monitor: Monitor = Monitor.NONE) -> Optional[DataSource]:

        time_range = TimeRangeLike.convert(time_range) if time_range else None
        region = PolygonLike.convert(region) if region else None
        var_names = VarNamesLike.convert(var_names) if var_names else None

        ds_id = local_name
        title = local_id

        local_store = DATA_STORE_REGISTRY.get_data_store('local')
        if not local_store:
            add_to_data_store_registry()
            local_store = DATA_STORE_REGISTRY.get_data_store('local')
        if not local_store:
            raise ValueError('Cannot initialize `local` DataStore')

        uuid = LocalDataStore.generate_uuid(ref_id=self.id, time_range=time_range, region=region, var_names=var_names)

        if not ds_id or len(ds_id) == 0:
            ds_id = "local.{}.{}".format(self.id, uuid)
            existing_ds_list = local_store.query(ds_id=ds_id)
            if len(existing_ds_list) == 1:
                return existing_ds_list[0]
        else:
            existing_ds_list = local_store.query(ds_id='local.%s' % ds_id)
            if len(existing_ds_list) == 1:
                if existing_ds_list[0].meta_info.get('uuid', None) == uuid:
                    return existing_ds_list[0]
                else:
                    raise ValueError('Datastore {} already contains dataset {}'.format(local_store.id, ds_id))

        local_meta_info = self.meta_info.copy()
        local_meta_info['ref_uuid'] = local_meta_info.get('uuid', None)
        local_meta_info['uuid'] = uuid

        local_ds = local_store.create_data_source(ds_id, title=title,
                                                  time_range=time_range, region=region, var_names=var_names,
                                                  meta_info=local_meta_info, lock_file=True)
        if local_ds:
            if not local_ds.is_complete:
                try:
                    self._make_local(local_ds, time_range, region, var_names, monitor=monitor)
                except Cancellation as c:
                    local_store.remove_data_source(local_ds)
                    raise c
                except Exception as e:
                    if local_ds.is_empty:
                        local_store.remove_data_source(local_ds)
                    raise e

            if local_ds.is_empty:
                local_store.remove_data_source(local_ds)
                return None

            local_store.register_ds(local_ds)
            return local_ds
        else:
            return None

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

    def to_json(self):
        return self._json_dict

    def __str__(self):
        return self.info_string

    def _repr_html_(self):
        return self.id

    def __repr__(self):
        return self.id


class _DownloadStatistics:
    def __init__(self, bytes_total):
        self.bytes_total = bytes_total
        self.bytes_done = 0
        self.start_time = datetime.now()

    def handle_chunk(self, chunk_size: int):
        self.bytes_done += chunk_size

    def __str__(self):
        seconds = (datetime.now() - self.start_time).seconds
        if seconds > 0:
            mb_per_sec = self._to_megas(self.bytes_done) / seconds
        else:
            mb_per_sec = 0.
        percent = 100. * self.bytes_done / self.bytes_total
        return "%d of %d MB @ %.3f MB/s, %.1f%% complete" % \
               (self._to_megas(self.bytes_done), self._to_megas(self.bytes_total), mb_per_sec, percent)

    @staticmethod
    def _to_megas(bytes_count: int) -> float:
        return bytes_count / (1000. * 1000.)


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
        error_message = f"Failed accessing CCI ODP service {self._catalogue_url}"
        try:
            if not self._catalogue_service:
                self._init_service()
            if not self._catalogue:
                self._build_catalogue(monitor.child(1))
            return self._catalogue
        except urllib.error.HTTPError as e:
            raise DataAccessError(f"{error_message}: {e}") from e
        except (urllib.error.URLError, socket.timeout) as e:
            raise NetworkError(f"{error_message}: {e}") from e
        except OSError as e:
            raise DataAccessError(f"{error_message}: {e}") from e

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
