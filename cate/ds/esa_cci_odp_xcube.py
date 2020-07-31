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
import json
import os
import socket
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Sequence, Optional, Any, Dict, List, Union
from urllib.error import URLError, HTTPError

import pandas as pd

import xcube.core.store as x_store
from xcube_cci.dataaccess import CciOdpDataStore

from cate.conf import get_config_value
from cate.conf import get_data_stores_path
from cate.conf.defaults import NETCDF_COMPRESSION_LEVEL
from cate.core.ds import DATA_STORE_REGISTRY, DataStore, DataSource, DataStoreNotice, NetworkError
from cate.core.types import PolygonLike, TimeRange, TimeRangeLike, VarNamesLike
from cate.ds.local import add_to_data_store_registry, LocalDataSource, LocalDataStore
from cate.core.opimpl import adjust_spatial_attrs_impl
from cate.core.opimpl import normalize_impl
from cate.core.opimpl import subset_spatial_impl
from cate.util.monitor import Cancellation, Monitor


ESA_CCI_ODP_DATA_STORE_ID = 'esa_cci_odp_os_xcube'
_TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S'

__author__ = "Norman Fomferra (Brockmann Consult GmbH), " \
             "Tonio Fincke (Brockmann Consult GmbH), " \
             "Marco Zühlke (Brockmann Consult GmbH), " \
             "Chris Bernat (Telespazio VEGA UK Ltd), " \
             "Paolo Pesciullesi (Telespazio VEGA UK Ltd)"


def add_data_store():
    DATA_STORE_REGISTRY.add_data_store(EsaCciOdpDataStore())


def get_data_store_path():
    return os.environ.get('CATE_LOCAL_DATA_STORE_PATH',
                          os.path.join(get_data_stores_path(), 'local'))


def get_metadata_store_path():
    return os.environ.get('CATE_ESA_CCI_ODP_DATA_STORE_PATH',
                          os.path.join(get_data_stores_path(), ESA_CCI_ODP_DATA_STORE_ID))


def _load_or_fetch_json(fetch_json_function,
                        fetch_json_args: list = None,
                        fetch_json_kwargs: dict = None,
                        cache_used: bool = False,
                        cache_dir: str = None,
                        cache_json_filename: str = None,
                        cache_timestamp_filename: str = None,
                        cache_expiration_days: float = 1.0) -> Union[Sequence, Dict]:
    """
    Return (JSON) value of fetch_json_function or return value of a cached JSON file.
    """
    json_obj = _load_json_obj(cache_used, cache_dir, cache_json_filename, cache_timestamp_filename, cache_expiration_days)
    cache_json_file = None

    if json_obj is None:
        # noinspection PyArgumentList
        try:
            # noinspection PyArgumentList
            json_obj = fetch_json_function(*(fetch_json_args or []), **(fetch_json_kwargs or {}))
            if cache_used:
                os.makedirs(cache_dir, exist_ok=True)
                cache_json_file = os.path.join(cache_dir, cache_json_filename)
                cache_timestamp_file = os.path.join(cache_dir, cache_timestamp_filename)
                # noinspection PyUnboundLocalVariable
                with open(cache_json_file, "w") as fp:
                    fp.write(json.dumps(json_obj, indent='  '))
                # noinspection PyUnboundLocalVariable
                with open(cache_timestamp_file, "w") as fp:
                    fp.write(datetime.utcnow().strftime(_TIMESTAMP_FORMAT))
        except Exception as e:
            if cache_json_file and os.path.exists(cache_json_file):
                with open(cache_json_file) as fp:
                    json_text = fp.read()
                    json_obj = json.loads(json_text)
            else:
                raise e

    return json_obj


def _load_json_obj(cache_used: bool = False,
                    cache_dir: str = None,
                    cache_json_filename: str = None,
                    cache_timestamp_filename: str = None,
                    cache_expiration_days: float = 1.0):
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
                    return json.loads(json_text)

    return None


def _load_or_fetch_jsons(fetch_json_function,
                         fetch_json_args_list: list = list,
                         fetch_json_kwargs: dict = None,
                         cache_used: bool = False,
                         cache_dir: str = None,
                         cache_json_filenames: list = list,
                         cache_timestamp_filenames: list = list,
                         cache_expiration_days: float = 1.0) -> Union[Sequence, Dict]:
    """
    Return (JSON) value of fetch_json_function or return value of a cached JSON file.
    """
    json_objs = {}
    still_to_fetch = []
    still_to_cache_name = []
    still_to_stamp_name = []
    for i, cache_json_filename in enumerate(cache_json_filenames):
        cache_timestamp_filename = cache_timestamp_filenames[i]
        json_obj = _load_json_obj(cache_used,
                                  cache_dir,
                                  cache_json_filename,
                                  cache_timestamp_filename,
                                  cache_expiration_days)
        if json_obj:
            json_objs[fetch_json_args_list[i]] = json_obj
        else:
            still_to_fetch.append(fetch_json_args_list[i])
            still_to_cache_name.append(cache_json_filename)
            still_to_stamp_name.append(cache_timestamp_filename)

    if len(still_to_fetch) > 0:
    # if json_obj is None:
        # noinspection PyArgumentList
        # try:
            # noinspection PyArgumentList
        fetched_json_objs = fetch_json_function(*(still_to_fetch or []), **(fetch_json_kwargs or {}))
        for i, to_fetch in enumerate(still_to_fetch):
            json_obj = fetched_json_objs[i]
            json_objs[to_fetch] = json_obj
            if cache_used:
                os.makedirs(cache_dir, exist_ok=True)
                cache_json_file = os.path.join(cache_dir, still_to_cache_name[i])
                cache_timestamp_file = os.path.join(cache_dir, still_to_stamp_name[i])
                # noinspection PyUnboundLocalVariable
                with open(cache_json_file, "w") as fp:
                    fp.write(json.dumps(json_obj, indent='  '))
                # noinspection PyUnboundLocalVariable
                with open(cache_timestamp_file, "w") as fp:
                    fp.write(datetime.utcnow().strftime(_TIMESTAMP_FORMAT))
    return json_objs


class EsaCciOdpDataStore(DataStore):
    # noinspection PyShadowingBuiltins
    def __init__(self,
                 id: str = 'esa_cci_odp_os_xcube',
                 title: str = 'ESA CCI Open Data Portal (xcube access)',
                 index_cache_used: bool = True,
                 index_cache_expiration_days: float = 1.0,
                 index_cache_update_tag: str = None,
                 meta_data_store_path: str = get_metadata_store_path()
                 ):
        super().__init__(id, title=title, is_local=False)
        self._index_cache_used = index_cache_used
        self._index_cache_expiration_days = index_cache_expiration_days
        self._index_cache_update_tag = index_cache_update_tag
        self._metadata_store_path = meta_data_store_path
        self._data_sources = []
        self._data_ids = []
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

    def _get_data_ids(self) -> list:
        if not self._data_ids:
            self._load_index()
        return self._data_ids

    def _fetch_data_ids(self) -> dict:
        data_id_dict = {}
        data_ids_iter = self._store.get_data_ids()
        try:
            while (True):
                data_id_tuple = next(data_ids_iter)
                data_id_dict[data_id_tuple[0]] = data_id_tuple[1]
        except StopIteration:
            pass
        return data_id_dict

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

    def _init_data_sources(self):
        os.makedirs(self._metadata_store_path, exist_ok=True)
        if self._data_sources:
            return
        if not self._data_ids:
            self._load_index()
        if self._data_ids:
            descriptors = self._store.describe_datasets(self._data_ids)
            self._data_sources = []
            for descriptor in descriptors:
                self._data_sources.append(EsaCciOdpDataSource(self, self._store, descriptor.data_id, descriptor))
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
        return 'data-ids'

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

    def _load_index(self):
        self._data_ids = _load_or_fetch_json(self._fetch_data_ids,
                                             fetch_json_args=[],
                                             fetch_json_kwargs=dict(),
                                             cache_used=self.index_cache_used,
                                             cache_dir=self._metadata_store_path,
                                             cache_json_filename='data-ids.json',
                                             cache_timestamp_filename='data-ids-timestamp.txt',
                                             cache_expiration_days=self.index_cache_expiration_days)

    def _fetch_descriptor(self, data_id: str) -> dict:
        return self._store.describe_data(data_id).to_dict()

    def _fetch_descriptors(self, data_ids: List[str]) -> List[dict]:
        return [descriptor.to_dict() for descriptor in self._store.describe_datasets(data_ids)]

    def _load_or_fetch_descriptor(self, data_id: str) -> x_store.DataDescriptor:
        descriptor_as_dict = _load_or_fetch_json(self._fetch_descriptor,
                                                 fetch_json_args=[data_id],
                                                 fetch_json_kwargs=dict(),
                                                 cache_used=self.index_cache_used,
                                                 cache_dir=self._metadata_store_path,
                                                 cache_json_filename=self._get_data_id_filename(data_id),
                                                 cache_timestamp_filename=self._get_data_id_timestamp_filename(data_id),
                                                 cache_expiration_days=self.index_cache_expiration_days
                                                 )
        return x_store.DatasetDescriptor.from_dict(descriptor_as_dict)

    def _get_data_id_filename(self, data_id: str) -> str:
        formatted_data_id = data_id.replace('.', '-')
        return f'{formatted_data_id}.json'

    def _get_data_id_timestamp_filename(self, data_id: str) -> str:
        formatted_data_id = data_id.replace('.', '-')
        return f'{formatted_data_id}-timestamp.txt'

    def _load_or_fetch_descriptors(self, data_ids: str) -> dict:
        filenames = []
        timestamps = []
        for data_id in data_ids:
            filenames.append(self._get_data_id_filename(data_id))
            timestamps.append(self._get_data_id_timestamp_filename(data_id))
        descriptors_as_dicts = _load_or_fetch_jsons(self._fetch_descriptors,
                                                    fetch_json_args_list=[[data_ids]],
                                                    fetch_json_kwargs=dict(),
                                                    cache_used=self.index_cache_used,
                                                    cache_dir=self._metadata_store_path,
                                                    cache_json_filenames=filenames,
                                                    cache_timestamp_filenames=timestamps,
                                                    cache_expiration_days=self.index_cache_expiration_days
                                                    )
        return [x_store.DatasetDescriptor.from_dict(descriptor_as_dict) for descriptor_as_dict in descriptors_as_dicts]



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
    def __init__(self, data_store: EsaCciOdpDataStore, cci_store: x_store.DataStore, data_id: str,
                 descriptor: x_store.DataDescriptor = None):
        super(EsaCciOdpDataSource, self).__init__()
        self._data_store = data_store
        self._cci_store = cci_store
        self._datasource_id = data_id
        self._descriptor = descriptor
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
        self._ensure_descriptor_read()
        if self._descriptor.bbox:
            return PolygonLike.convert(self._descriptor.bbox)
        return None

    def temporal_coverage(self, monitor: Monitor = Monitor.NONE) -> Optional[TimeRange]:
        self._ensure_descriptor_read()
        if self._descriptor.time_range:
            return TimeRangeLike.convert("{},{}".format(self._descriptor.time_range[0],
                                                        self._descriptor.time_range[1]))
        return None

    @property
    def variables_info(self):
        self._ensure_descriptor_read()
        variables = []
        if self._descriptor.data_vars:
            for variable_descriptor in self._descriptor.data_vars:
                variables.append(dict(name=variable_descriptor.name,
                                      units=variable_descriptor.attrs.get('units', '') if variable_descriptor.attrs else None,
                                      long_name=variable_descriptor.attrs.get('long_name', '')  if variable_descriptor.attrs else None))
        return variables

    @property
    def title(self) -> Optional[str]:
        self._ensure_descriptor_read()
        if self._descriptor.attrs:
            return self._descriptor.attrs.get('title', '')
        return ''

    @property
    def meta_info(self) -> OrderedDict:
        # noinspection PyBroadException
        self._ensure_descriptor_read()
        if not self._meta_info:
            self._meta_info = OrderedDict()
            for name in INFO_FIELD_NAMES:
                value = self._descriptor.attrs.get(name, None)
                # Many values in the index JSON are one-element lists: turn them into scalars
                if isinstance(value, list) and len(value) == 1:
                    value = value[0]
                if value is not None:
                    self._meta_info[name] = value
            self._meta_info['variables'] = self.variables_info
        return self._meta_info

    @property
    def cache_info(self) -> OrderedDict:
        return OrderedDict()

    def _ensure_descriptor_read(self):
        if self._descriptor:
            return
        self._descriptor = self._data_store._load_or_fetch_descriptor(self._datasource_id)

    def open_dataset(self,
                     time_range: TimeRangeLike.TYPE = None,
                     region: PolygonLike.TYPE = None,
                     var_names: VarNamesLike.TYPE = None,
                     protocol: str = None,
                     monitor: Monitor = Monitor.NONE
                     ) -> Any:
        time_range = TimeRangeLike.convert(time_range) if time_range else None
        var_names = VarNamesLike.convert(var_names) if var_names else None
        bbox = PolygonLike.convert(region).bounds
        return self._cci_store.open_data(data_id=self._datasource_id,
                                         variable_names=var_names,
                                         time_range=[datetime.strftime(time_range[0], '%Y%m%d%H%M%S'),
                                                     datetime.strftime(time_range[1], '%Y%m%d%H%M%S')],
                                         bbox=list(bbox))

    def _update_local_ds(self, local_ds: LocalDataSource, time_range: TimeRangeLike.TYPE = None,
                         region: PolygonLike.TYPE = None, var_names: VarNamesLike.TYPE = None,
                         monitor: Monitor = Monitor.NONE):
        time_range = TimeRangeLike.convert(time_range)
        var_names = VarNamesLike.convert(var_names)
        local_path = os.path.join(local_ds.data_store.data_store_path, local_ds.id)
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        remote_ds = self.open_dataset(time_range=time_range, region=region, var_names=var_names, monitor=monitor)
        do_update_of_verified_time_coverage_start_once = True
        verified_time_coverage_start = None
        verified_time_coverage_end = None
        compression_level = get_config_value('NETCDF_COMPRESSION_LEVEL', NETCDF_COMPRESSION_LEVEL)
        compression_enabled = True if compression_level > 0 else False
        encoding_update = dict()
        if compression_enabled:
            encoding_update.update({'zlib': True, 'complevel': compression_level})
        # update region meta info
        if region:
            remote_ds = normalize_impl(remote_ds)
            remote_ds = subset_spatial_impl(remote_ds, region)
            remote_ds = adjust_spatial_attrs_impl(remote_ds, allow_point=False)
            local_ds.meta_info['bbox_minx'] = remote_ds.attrs.get('geospatial_lon_min', '')
            local_ds.meta_info['bbox_maxx'] = remote_ds.attrs.get('geospatial_lon_max', '')
            local_ds.meta_info['bbox_maxy'] = remote_ds.attrs.get('geospatial_lat_max', '')
            local_ds.meta_info['bbox_miny'] = remote_ds.attrs.get('geospatial_lat_min', '')
        # update variables meta info
        variables_info = local_ds.meta_info.get('variables', [])
        local_ds.meta_info['variables'] = [var_info for var_info in variables_info
                                           if var_info.get('name')
                                           in remote_ds.variables.keys()
                                           and var_info.get('name')
                                           not in remote_ds.dims.keys()]
        with monitor.starting('Sync ' + self.id, total_work=remote_ds.time.size):
            for idx, time_stamp in enumerate(remote_ds.time.data):
                child_monitor = monitor.child(work=1)
                time_slice = remote_ds.sel(time=time_stamp)
                time = pd.to_datetime(str(time_stamp)).strftime('%Y%m%d')
                file_name = f"{remote_ds.title.split(' ')[0]}-{time}"
                local_filepath = os.path.join(local_path, file_name)
                with child_monitor.starting(label=file_name, total_work=100):
                    try:
                        remote_dataset_root = time_slice
                        child_monitor.progress(work=20)
                        if compression_enabled:
                            for sel_var_name in remote_ds.variables.keys():
                                time_slice.variables.get(sel_var_name).encoding.update(encoding_update)
                        time_slice_history = time_slice.attrs.pop('history')
                        if time_slice_history:
                            time_slice.attrs['history'] = json.dumps(time_slice_history)
                        to_netcdf_attempts = 0
                        format = 'NETCDF4'
                        engine = 'h5netcdf'
                        while to_netcdf_attempts < 2:
                            try:
                                to_netcdf_attempts += 1
                                # Note: we are using engine='h5netcdf' here because the default engine='netcdf4'
                                # causes crashes in file "netCDF4/_netCDF4.pyx" with currently used netcdf4-1.4.2 conda
                                # package from conda-forge. This occurs whenever remote_dataset.to_netcdf() is called a
                                # second time in this loop.
                                # Probably related to https://github.com/pydata/xarray/issues/2560.
                                # And probably fixes Cate issues #823, #822, #818, #816, #783.
                                time_slice.to_netcdf(local_filepath, format=format, engine=engine)
                                break
                            except AttributeError as e:
                                if to_netcdf_attempts == 1:
                                    format = 'NETCDF3_64BIT'
                                    engine = None
                                    continue
                                raise self._cannot_access_error(time_range, region, var_names,
                                                                verb="synchronize", cause=e) from e
                        child_monitor.progress(work=75)
                        time_bounds = time_slice.time_bnds.data
                        start_time = pd.to_datetime(str(time_bounds[0])).strftime('%Y-%m-%dT%H:%M:%S')
                        end_time = pd.to_datetime(str(time_bounds[1])).strftime('%Y-%m-%dT%H:%M:%S')
                        local_ds.add_dataset(os.path.join(local_ds.id, file_name), (start_time, end_time))
                        if do_update_of_verified_time_coverage_start_once:
                            verified_time_coverage_start = start_time
                            do_update_of_verified_time_coverage_start_once = False
                        verified_time_coverage_end = end_time
                        child_monitor.progress(work=5)
                        remote_dataset_root.close()
                        break
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
        remote_ds.close()
        local_ds.meta_info['temporal_coverage_start'] = verified_time_coverage_start
        local_ds.meta_info['temporal_coverage_end'] = verified_time_coverage_end
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

    def __str__(self):
        return self.info_string

    def _repr_html_(self):
        return self.id

    def __repr__(self):
        return self.id
