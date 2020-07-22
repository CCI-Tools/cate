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

This plugin module adds the ESA CCI Open Data Portal's (ODP) service to the data store registry.
As of April 2020, the ODP service provides a OpenSearch-compatible catalogue, which is utilised in this implementation.

Verification
============

The module's unit-tests are located in
`test/ds/test_esa_cci_odp.py <https://github.com/CCI-Tools/cate/blob/master/test/ds/test_esa_cci_odp.py>`_
and may be executed using
``$ py.test test/ds/test_esa_cci_odp_legacy.py --cov=cate/ds/esa_cci_odp.py`` for extra code coverage information.

Components
==========
"""
import aiofiles
import aiohttp
import asyncio
import itertools
import json
import logging
import os
import re
import socket
import urllib.parse
import urllib.request
from collections import OrderedDict
from datetime import datetime, timedelta
import lxml.etree as etree
from typing import Sequence, Tuple, Optional, Any, Dict, List, Union
from urllib.error import URLError, HTTPError

import pandas as pd
import xarray as xr

from xcube_cci.dataaccess import CciOdpDataStore

from cate.conf import get_config_value, get_data_stores_path
from cate.conf.defaults import NETCDF_COMPRESSION_LEVEL
from cate.core.ds import DATA_STORE_REGISTRY, NetworkError, DataStore, DataSource, Schema, open_xarray_dataset, \
    DataStoreNotice
from cate.core.opimpl import subset_spatial_impl, normalize_impl, adjust_spatial_attrs_impl
from cate.core.types import PolygonLike, TimeLike, TimeRange, TimeRangeLike, VarNamesLike
from cate.ds.local import add_to_data_store_registry, LocalDataSource, LocalDataStore
from cate.util.monitor import Cancellation, Monitor


ESA_CCI_ODP_DATA_STORE_ID = 'esa_cci_odp_os'

__author__ = "Norman Fomferra (Brockmann Consult GmbH), " \
             "Tonio Fincke (Brockmann Consult GmbH), " \
             "Marco Zühlke (Brockmann Consult GmbH), " \
             "Chris Bernat (Telespazio VEGA UK Ltd), " \
             "Paolo Pesciullesi (Telespazio VEGA UK Ltd)"


def get_data_store_path():
    return os.environ.get('CATE_LOCAL_DATA_STORE_PATH',
                          os.path.join(get_data_stores_path(), 'local'))


def get_metadata_store_path():
    return os.environ.get('CATE_ESA_CCI_ODP_DATA_STORE_PATH',
                          os.path.join(get_data_stores_path(), ESA_CCI_ODP_DATA_STORE_ID))


class EsaCciOdpDataStore(DataStore):
    # noinspection PyShadowingBuiltins
    def __init__(self,
                 id: str = 'esa_cci_odp_os',
                 title: str = 'ESA CCI Open Data Portal',
                 index_cache_used: bool = True,
                 index_cache_expiration_days: float = 1.0,
                 index_cache_json_dict: dict = None,
                 index_cache_update_tag: str = None,
                 meta_data_store_path: str = get_metadata_store_path()
                 ):
        super().__init__(id, title=title, is_local=False)
        self._index_cache_used = index_cache_used
        self._index_cache_expiration_days = index_cache_expiration_days
        self._catalogue = index_cache_json_dict
        self._index_cache_update_tag = index_cache_update_tag
        self._metadata_store_path = meta_data_store_path
        self._data_sources = []
        self._drs_ids = []
        self._store = CciOdpDataStore(normalize_data=True)


    @property
    def description(self) -> Optional[str]:
        """
        Return a human-readable description for this data store as plain text.

        The text may use Markdown formatting.
        """
        return ("This data store represents the [ESA CCI Open Data Portal](http://cci.esa.int/data)"
                " in the CCI Toolbox.\n"
                "It currently provides all CCI data that are published through an OpenSearch interface. "
                "The store will be extended shortly to also provide TIFF and Shapefile Data, see usage "
                "notes.\n"
                "Remote data downloaded to your computer is made available through the *File Data Store*.")

    @property
    def notices(self) -> Optional[List[DataStoreNotice]]:
        """
        Return an optional list of notices for this data store that can be used to inform users about the
        conventions, standards, and data extent used in this data store or upcoming service outages.
        """
        return [
            DataStoreNotice("terminologyClarification",
                            "Terminology Clarification",
                            "The ESA CCI Open Data Portal (ODP) utilises an "
                            "[ontology](http://vocab-test.ceda.ac.uk/ontology/cci/cci-content/index.html) whose terms "
                            "might slightly differ from the ones used in this software."
                            "\n"
                            "For example, a *Dataset* in the CCI terminology may refer to all data products "
                            "generated by a certain CCI project using a specific configuration of algorithms "
                            "and auxiliary data."
                            "\n"
                            "In this software, a *Data Source* refers to a subset (a file set) "
                            "of a given ODP dataset whose data share a common spatio-temporal grid and/or share "
                            "other common properties, e.g. the instrument used for the original measurements."
                            "\n"
                            "In addition, Cate uses the term *Dataset* to represent in-memory "
                            "instances of gridded data sources or subsets of them.",
                            intent="primary",
                            icon="info-sign"),
            DataStoreNotice("dataCompleteness",
                            "Data Completeness",
                            "This data store currently provides **only a subset of all datasets** provided by the "
                            "ESA CCI Open Data Portal (ODP), namely gridded datasets originally stored in NetCDF "
                            "format."
                            "\n"
                            "In upcoming versions of Cate, the ODP data store will also allow for browsing "
                            "and accessing the remaining ODP datasets. This includes gridded data in TIFF format and "
                            "also vector data using ESRI Shapefile format."
                            "\n"
                            "For time being users can download the missing vector data from the "
                            "[ODP FTP server](http://cci.esa.int/data#ftp) `ftp://anon-ftp.ceda.ac.uk/neodc/esacci/` "
                            "and then use operation `read_geo_data_frame()` in Cate to read the "
                            "downloaded Shapefiles:"
                            "\n"
                            "* CCI Glaciers in FTP directory `glaciers`\n"
                            "* CCI Ice Sheets in FTP directories `ice_sheets_antarctica` and `ice_sheets_greenland`\n",
                            intent="warning",
                            icon="warning-sign"),
        ]

    @property
    def index_cache_used(self):
        return self._index_cache_used

    @property
    def index_cache_expiration_days(self):
        return self._index_cache_expiration_days

    @property
    def data_store_path(self) -> str:
        return self._metadata_store_path

    def query(self, ds_id: str = None, query_expr: str = None, monitor: Monitor = Monitor.NONE) \
            -> Sequence['DataSource']:
        # if ds_id:
        #
        # self._store.search()
        asyncio.run(self._init_data_sources())
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
        diff_file = os.path.join(self._metadata_store_path, self._get_update_tag() + '-diff.json')

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
            frozen_file = os.path.join(self._metadata_store_path, self._get_update_tag() + '-freeze.json')
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

    async def _fetch_dataset_names(self, session):
        meta_info_dict = await _extract_metadata_from_odd_url(session, _OPENSEARCH_CEDA_ODD_URL)
        if 'drs_ids' in meta_info_dict:
            return meta_info_dict['drs_ids']

    async def _init_data_sources(self):
        os.makedirs(self._metadata_store_path, exist_ok=True)
        if self._data_sources:
            return
        if self._catalogue is None:
            await self._load_index()
        if not self._drs_ids:
            async with aiohttp.ClientSession() as session:
                self._drs_ids = await self._fetch_dataset_names(session)
        if self._catalogue:
            self._data_sources = []
            tasks = []
            for catalogue_item in self._catalogue:
                tasks.append(self._create_data_source(self._catalogue[catalogue_item], catalogue_item))
            await asyncio.gather(*tasks)
        self._check_source_diff()
        self._freeze_source()

    async def _create_data_source(self, json_dict: dict, datasource_id: str):
        local_metadata_dataset_dir = os.path.join(self._metadata_store_path, datasource_id)
        # todo set True when dimensions shall be read during meta data fetching
        meta_info = await _load_or_fetch_json(_fetch_meta_info,
                                              fetch_json_args=[datasource_id,
                                                               json_dict.get('odd_url', None),
                                                               json_dict.get('metadata_url', None),
                                                               json_dict.get('variables', []),
                                                               False],
                                              fetch_json_kwargs=dict(),
                                              cache_used=self.index_cache_used,
                                              cache_dir=local_metadata_dataset_dir,
                                              cache_json_filename='meta-info.json',
                                              cache_timestamp_filename='meta-info-timestamp.txt',
                                              cache_expiration_days=self.index_cache_expiration_days)
        drs_ids = self._get_as_list(meta_info, 'drs_id', 'drs_ids')
        for drs_id in drs_ids:
            if drs_id not in self._drs_ids:
                continue
            meta_info = meta_info.copy()
            meta_info.update(json_dict)
            self._adjust_json_dict(meta_info, drs_id)
            meta_info['cci_project'] = meta_info['ecv']
            meta_info['fid'] = datasource_id
            data_source = EsaCciOdpDataSource(self, meta_info, datasource_id, drs_id)
            self._data_sources.append(data_source)

    def _adjust_json_dict(self, json_dict: dict, drs_id: str):
        values = drs_id.split('.')
        self._adjust_json_dict_for_param(json_dict, 'time_frequency', 'time_frequencies',
                                         self._convert_time_from_drs_id(values[2]))
        self._adjust_json_dict_for_param(json_dict, 'processing_level', 'processing_levels', values[3])
        self._adjust_json_dict_for_param(json_dict, 'data_type', 'data_types', values[4])
        self._adjust_json_dict_for_param(json_dict, 'sensor_id', 'sensor_ids', values[5])
        self._adjust_json_dict_for_param(json_dict, 'platform_id', 'platform_ids', values[6])
        self._adjust_json_dict_for_param(json_dict, 'product_string', 'product_strings', values[7])
        self._adjust_json_dict_for_param(json_dict, 'product_version', 'product_versions', values[8])

    @staticmethod
    def _convert_time_from_drs_id(time_value: str) -> str:
        time_value_lookup = {'mon': 'month', 'month': 'month', 'yr': 'year', 'year': 'year', 'day': 'day',
                             'satellite-orbit-frequency': 'satellite-orbit-frequency', 'climatology': 'climatology'}
        if time_value in time_value_lookup:
            return time_value_lookup[time_value]
        if re.match('[0-9]+-[days|yrs]', time_value):
            split_time_value = time_value.split('-')
            return f'{split_time_value[0]} {split_time_value[1].replace("yrs", "years")}'
        raise ValueError('Unknown time frequency format')

    def _adjust_json_dict_for_param(self, json_dict: dict, single_name: str, list_name: str, param_value: str):
        json_dict[single_name] = param_value
        if list_name in json_dict:
            json_dict.pop(list_name)

    def _get_pretty_id(self, json_dict: dict, value_tuple: Tuple, drs_id: str) -> str:
        pretty_values = []
        for value in value_tuple:
            pretty_values.append(self._make_string_pretty(value))
        return f'esacci2.{json_dict["ecv"]}.{".".join(pretty_values)}.{drs_id}'

    def _make_string_pretty(self, string: str):
        string = string.replace(" ", "-")
        if string.startswith("."):
            string = string[1:]
        if string.endswith("."):
            string = string[:-1]
        if "." in string:
            string = string.replace(".", "-")
        return string

    def _get_as_list(self, meta_info: dict, single_name: str, list_name: str) -> List:
        if single_name in meta_info:
            return [meta_info[single_name]]
        if list_name in meta_info:
            return meta_info[list_name]
        return []

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
        frozen_file = os.path.join(self._metadata_store_path, self._get_update_tag() + '-freeze.json')
        diff_file = os.path.join(self._metadata_store_path, self._get_update_tag() + '-diff.json')
        deleted = []
        added = []

        if os.path.isfile(frozen_file):
            with open(frozen_file, 'r') as json_in:
                frozen_source = json.load(json_in)

            ds_new = set([ds.id for ds in self._data_sources])
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
        frozen_file = os.path.join(self._metadata_store_path, self._get_update_tag() + '-freeze.json')
        save_it = True
        now = datetime.now()
        if os.path.isfile(frozen_file):
            with open(frozen_file, 'r') as json_in:
                freezed_source = json.load(json_in)
            source_ref_time = pd.to_datetime(freezed_source['source_ref_time'])
            save_it = (now > source_ref_time + timedelta(days=1))

        if save_it:
            data = [ds.id for ds in self._data_sources]
            freezed_source = {'source_ref_time': str(now),
                              'data': data}
            with open(frozen_file, 'w+') as json_out:
                json.dump(freezed_source, json_out)

    async def _load_index(self):
        self._catalogue = await _load_or_fetch_json(_fetch_data_source_list_json,
                                                    fetch_json_args=[
                                                        _OPENSEARCH_CEDA_URL,
                                                        dict(parentIdentifier='cci')
                                                    ],
                                                    cache_used=self._index_cache_used,
                                                    cache_dir=self._metadata_store_path,
                                                    cache_json_filename='dataset-list.json',
                                                    cache_timestamp_filename='dataset-list-timestamp.json',
                                                    cache_expiration_days=self._index_cache_expiration_days
                                                    )


INFO_FIELD_NAMES = sorted(["title",
                           "abstract",
                           "licences",
                           "bbox_minx",
                           "bbox_miny",
                           "bbox_maxx",
                           "bbox_maxy",
                           "temporal_coverage_start",
                           "temporal_coverage_end",
                           "file_format",
                           "file_formats",
                           "publication_date",
                           "creation_date",
                           "platform_ids",
                           "platform_id",
                           "sensor_ids",
                           "sensor_id",
                           "processing_levels",
                           "processing_level",
                           "time_frequencies",
                           "time_frequency",
                           "ecv",
                           "institute",
                           "institutes",
                           "product_string",
                           "product_strings",
                           "product_version",
                           "product_versions",
                           "data_type",
                           "data_types",
                           "cci_project"
                           ])


class EsaCciOdpDataSource(DataSource):
    def __init__(self,
                 data_store: EsaCciOdpDataStore,
                 json_dict: dict,
                 raw_datasource_id: str,
                 datasource_id: str,
                 schema: Schema = None):
        super(EsaCciOdpDataSource, self).__init__()
        self._raw_id = raw_datasource_id
        self._datasource_id = datasource_id
        self._data_store = data_store
        self._json_dict = json_dict
        self._schema = schema
        self._file_list = None
        self._meta_info = None
        self._temporal_coverage = None

    @property
    def id(self) -> str:
        return self._datasource_id

    @property
    def data_store(self) -> EsaCciOdpDataStore:
        return self._data_store

    @property
    def spatial_coverage(self) -> Optional[PolygonLike]:
        if self._json_dict \
                and self._json_dict.get('bbox_minx', None) and self._json_dict.get('bbox_miny', None) \
                and self._json_dict.get('bbox_maxx', None) and self._json_dict.get('bbox_maxy', None):
            return PolygonLike.convert([
                self._json_dict.get('bbox_minx'),
                self._json_dict.get('bbox_miny'),
                self._json_dict.get('bbox_maxx'),
                self._json_dict.get('bbox_maxy')
            ])
        return None

    def temporal_coverage(self, monitor: Monitor = Monitor.NONE) -> Optional[TimeRange]:
        if not self._temporal_coverage:
            temp_coverage_start = self._json_dict.get('temporal_coverage_start', None)
            temp_coverage_end = self._json_dict.get('temporal_coverage_end', None)
            if temp_coverage_start and temp_coverage_end:
                self._temporal_coverage = TimeRangeLike.convert("{},{}".format(temp_coverage_start, temp_coverage_end))
            else:
                self.update_file_list(monitor)
        if self._temporal_coverage:
            return self._temporal_coverage
        return None

    @property
    def variables_info(self):
        variables = []
        coordinate_variable_names = ['lat', 'lon', 'time', 'lat_bnds', 'lon_bnds', 'time_bnds', 'crs']
        for variable in self._json_dict['variables']:
            if 'variable_infos' in self._meta_info and variable['name'] in self._meta_info['variable_infos'] and \
                    len(self._meta_info['variable_infos'][variable['name']]['dimensions']) == 0:
                continue
            if 'dimensions' in self._meta_info and variable['name'] in self._meta_info['dimensions']:
                continue
            if variable['name'] not in coordinate_variable_names:
                variables.append(variable)
        return variables

    @property
    def schema(self) -> Schema:
        return self._schema

    @property
    def title(self) -> Optional[str]:
        return self._json_dict['title']

    @property
    def meta_info(self) -> OrderedDict:
        # noinspection PyBroadException
        if not self._meta_info:
            self._meta_info = OrderedDict()
            for name in INFO_FIELD_NAMES:
                value = self._json_dict.get(name, None)
                # Many values in the index JSON are one-element lists: turn them into scalars
                if isinstance(value, list) and len(value) == 1:
                    value = value[0]
                if value is not None:
                    self._meta_info[name] = value
            self._meta_info['variables'] = self.variables_info
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

    def update_file_list(self, monitor: Monitor = Monitor.NONE) -> None:
        self._file_list = None
        asyncio.run(self._init_file_list(monitor))

    def local_dataset_dir(self):
        return os.path.join(get_data_store_path(), self._raw_id, self._datasource_id)

    def local_metadata_dataset_dir(self):
        return os.path.join(get_metadata_store_path(), self._raw_id, self._datasource_id)

    def _find_files(self, time_range):
        requested_start_date, requested_end_date = time_range if time_range else (None, None)
        asyncio.run(self._init_file_list())
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
        # we need to add this very ugly part so that netcdf works with all files
        files = [f + '#fillmismatch' for f in files]
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

    def _update_local_ds(self, local_ds: LocalDataSource, time_range: TimeRangeLike.TYPE = None,
                         region: PolygonLike.TYPE = None, var_names: VarNamesLike.TYPE = None,
                         monitor: Monitor = Monitor.NONE):
        time_range = TimeRangeLike.convert(time_range)
        var_names = VarNamesLike.convert(var_names)
        local_path = os.path.join(local_ds.data_store.data_store_path, local_ds.id)
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        selected_file_list = self._find_files(time_range)
        if not selected_file_list:
            raise self._empty_error(time_range)
        if region or var_names:
            self._update_ds_using_opendap(local_path, local_ds, selected_file_list, time_range, var_names, region,
                                          monitor)
        else:
            self._update_ds_using_http(local_path, local_ds, selected_file_list, time_range, region, var_names, monitor)
        local_ds.save(True)

    def _update_ds_using_opendap(self, local_path, local_ds, selected_file_list, time_range, var_names, region,
                                 monitor):
        do_update_of_verified_time_coverage_start_once = True
        do_update_of_variables_meta_info_once = True
        do_update_of_region_meta_info_once = True
        verified_time_coverage_start = None
        verified_time_coverage_end = None
        compression_level = get_config_value('NETCDF_COMPRESSION_LEVEL', NETCDF_COMPRESSION_LEVEL)
        compression_enabled = True if compression_level > 0 else False
        encoding_update = dict()
        if compression_enabled:
            encoding_update.update({'zlib': True, 'complevel': compression_level})
        files = self._get_urls_list(selected_file_list, _ODP_PROTOCOL_OPENDAP)
        with monitor.starting('Sync ' + self.id, total_work=len(files)):
            for idx, dataset_uri in enumerate(files):
                child_monitor = monitor.child(work=1)
                file_name = os.path.basename(dataset_uri)
                local_filepath = os.path.join(local_path, file_name)
                time_coverage_start = selected_file_list[idx][1]
                time_coverage_end = selected_file_list[idx][2]
                with child_monitor.starting(label=file_name, total_work=100):
                    attempts = 0
                    to_append = ''
                    while attempts < 2:
                        try:
                            attempts += 1
                            remote_dataset = xr.open_dataset(dataset_uri + to_append)
                            remote_dataset_root = remote_dataset
                            child_monitor.progress(work=20)
                            if var_names:
                                remote_dataset = remote_dataset.drop_vars(
                                    [var_name for var_name in remote_dataset.data_vars.keys()
                                     if var_name not in var_names]
                                )
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
                            to_netcdf_attempts = 0
                            format = 'NETCDF4'
                            # format = 'NETCDF3_64BIT'
                            engine = 'h5netcdf'
                            # engine = None
                            while to_netcdf_attempts < 2:
                                try:
                                    to_netcdf_attempts += 1
                                    # Note: we are using engine='h5netcdf' here because the default engine='netcdf4'
                                    # causes crashes in file "netCDF4/_netCDF4.pyx" with currently used netcdf4-1.4.2 conda
                                    # package from conda-forge. This occurs whenever remote_dataset.to_netcdf() is called a
                                    # second time in this loop.
                                    # Probably related to https://github.com/pydata/xarray/issues/2560.
                                    # And probably fixes Cate issues #823, #822, #818, #816, #783.
                                    remote_dataset.to_netcdf(local_filepath, format=format, engine=engine)
                                except AttributeError as e:
                                    if to_netcdf_attempts == 1:
                                        format = 'NETCDF3_64BIT'
                                        engine = None
                                        continue
                                    raise self._cannot_access_error(time_range, region, var_names,
                                                                    verb="synchronize", cause=e) from e
                            child_monitor.progress(work=75)

                            if do_update_of_variables_meta_info_once:
                                variables_info = local_ds.meta_info.get('variables', [])
                                local_ds.meta_info['variables'] = [var_info for var_info in variables_info
                                                                   if var_info.get('name')
                                                                   in remote_dataset.variables.keys()
                                                                   and var_info.get('name')
                                                                   not in remote_dataset.dims.keys()]
                                do_update_of_variables_meta_info_once = False
                            local_ds.add_dataset(os.path.join(local_ds.id, file_name),
                                                 (time_coverage_start, time_coverage_end))
                            if do_update_of_verified_time_coverage_start_once:
                                verified_time_coverage_start = time_coverage_start
                                do_update_of_verified_time_coverage_start_once = False
                            verified_time_coverage_end = time_coverage_end
                            child_monitor.progress(work=5)
                            remote_dataset_root.close()
                        except HTTPError as e:
                            if attempts == 1:
                                to_append = '#fillmismatch'
                                continue
                            raise self._cannot_access_error(time_range, region, var_names,
                                                            verb="synchronize", cause=e) from e
                        except (URLError, socket.timeout) as e:
                            if attempts == 1:
                                to_append = '#fillmismatch'
                                continue
                            raise self._cannot_access_error(time_range, region, var_names,
                                                            verb="synchronize", cause=e,
                                                            error_cls=NetworkError) from e
                        except OSError as e:
                            if attempts == 1:
                                to_append = '#fillmismatch'
                                continue
                            raise self._cannot_access_error(time_range, region, var_names,
                                                            verb="synchronize", cause=e) from e
        local_ds.meta_info['temporal_coverage_start'] = TimeLike.format(verified_time_coverage_start)
        local_ds.meta_info['temporal_coverage_end'] = TimeLike.format(verified_time_coverage_end)

    def _update_ds_using_http(self, local_path, local_ds, selected_file_list, time_range, region, var_names, monitor):
        do_update_of_verified_time_coverage_start_once = True
        verified_time_coverage_start = None
        verified_time_coverage_end = None
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
                        actual_url = url[_ODP_PROTOCOL_HTTP]
                        _LOG.info(f"Downloading {actual_url} to {dataset_file}")
                        try:
                            urllib.request.urlretrieve(actual_url, filename=dataset_file, reporthook=reporthook)
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
                    file_number += 1
                    local_ds.add_dataset(os.path.join(local_ds.id, filename), (coverage_from, coverage_to))
                    if do_update_of_verified_time_coverage_start_once:
                        verified_time_coverage_start = coverage_from
                        do_update_of_verified_time_coverage_start_once = False
                    verified_time_coverage_end = coverage_to
        local_ds.meta_info['temporal_coverage_start'] = TimeLike.format(verified_time_coverage_start)
        local_ds.meta_info['temporal_coverage_end'] = TimeLike.format(verified_time_coverage_end)

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
        local_meta_info['uuid'] = uuid

        local_ds = local_store.create_data_source(ds_id, title=title,
                                                  time_range=time_range, region=region, var_names=var_names,
                                                  meta_info=local_meta_info, lock_file=True)
        if local_ds:
            if not local_ds.is_complete:
                try:
                    self._update_local_ds(local_ds, time_range, region, var_names, monitor=monitor)
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

    async def ensure_meta_info_set(self):
        if self._json_dict:
            return
        # todo set True when dimensions shall be read during meta data fetching
        self._json_dict = await _load_or_fetch_json(_fetch_meta_info,
                                                         fetch_json_args=[self._raw_id,
                                                                          self._json_dict['odd_url'],
                                                                          self._json_dict['metadata_url'],
                                                                          self._json_dict['variables'],
                                                                          # True],
                                                                          False],
                                                         fetch_json_kwargs=dict(),
                                                         cache_used=self._data_store.index_cache_used,
                                                         cache_dir=self.local_metadata_dataset_dir(),
                                                         cache_json_filename='meta-info.json',
                                                         cache_timestamp_filename='meta-info-timestamp.txt',
                                                         cache_expiration_days=self._data_store.index_cache_expiration_days)

    async def _init_file_list(self, monitor: Monitor = Monitor.NONE):
        await self.ensure_meta_info_set()
        if self._file_list:
            return
        file_list = await _load_or_fetch_json(_fetch_file_list_json,
                                              fetch_json_args=[self._raw_id, self._datasource_id],
                                              fetch_json_kwargs=dict(monitor=monitor),
                                              cache_used=self._data_store.index_cache_used,
                                              cache_dir=self.local_metadata_dataset_dir(),
                                              cache_json_filename='file-list.json',
                                              cache_timestamp_filename='file-list-timestamp.txt',
                                              cache_expiration_days=self._data_store.index_cache_expiration_days)

        time_frequency = self._json_dict.get('time_frequency', None)
        if time_frequency is None and 'time_frequencies' in self._json_dict:
            time_frequency = self._json_dict.get('time_frequencies')[0]
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
                file_start_date = datetime.strptime(file_rec[1].split('.')[0], _TIMESTAMP_FORMAT)
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
