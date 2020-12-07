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

This module provides Cate's data access API.

Technical Requirements
======================

**Query data store**

:Description: Allow querying registered ECV data stores using a simple function that takes a set of query parameters
    and returns data source identifiers that can be used to open respective ECV dataset in the Cate.

:URD-Source:
    * CCIT-UR-DM0006: Data access to ESA CCI
    * CCIT-UR-DM0010: The data module shall have the means to attain meta-level status information per ECV type
    * CCIT-UR-DM0013: The CCI Toolbox shall allow filtering

----

**Add data store**

:Description: Allow adding of user defined data stores specifying the access protocol and the layout of the data.
    These data stores can be used to access datasets.

:URD-Source:
    * CCIT-UR-DM0011: Data access to non-CCI data

----

**Open dataset**

:Description: Allow opening an ECV dataset given an identifier returned by the *data store query*.
   The dataset returned complies to the Cate common data model.
   The dataset to be returned can optionally be constrained in time and space.

:URD-Source:
    * CCIT-UR-DM0001: Data access and input
    * CCIT-UR-DM0004: Open multiple inputs
    * CCIT-UR-DM0005: Data access using different protocols>
    * CCIT-UR-DM0007: Open single ECV
    * CCIT-UR-DM0008: Open multiple ECV
    * CCIT-UR-DM0009: Open any ECV
    * CCIT-UR-DM0012: Open different formats


Verification
============

The module's unit-tests are located in
`test/test_ds.py <https://github.com/CCI-Tools/cate/blob/master/test/test_ds.py>`_
and may be executed using ``$ py.test test/test_ds.py --cov=cate/core/ds.py`` for extra code coverage information.


Components
==========
"""

import datetime
import glob
import itertools
import json
import logging
import os
import re
from abc import ABCMeta, abstractmethod
from typing import Sequence, Optional, Union, Any, Dict, Set, List, Tuple

import xarray as xr
import xcube.core.store as xcube_store
import xcube.util.extension as xcube_extension

from cate.conf import get_data_stores_path
from .cdm import get_lon_dim_name, get_lat_dim_name
from .opimpl import normalize_missing_time, normalize_coord_vars, normalize_impl, subset_spatial_impl
from .types import PolygonLike, TimeRangeLike, VarNamesLike, ValidationError
from ..util.monitor import Monitor

_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"

__author__ = "Chris Bernat (Telespazio VEGA UK Ltd), ", \
             "Tonio Fincke (Brockmann Consult GmbH), " \
             "Norman Fomferra (Brockmann Consult GmbH), " \
             "Marco Zühlke (Brockmann Consult GmbH)"

URL_REGEX = re.compile(
    r'^(?:http|ftp)s?://'  # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain
    r'localhost|'  # localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
    r'(?::\d+)?'  # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)

_LOG = logging.getLogger('cate')


class DataAccessWarning(UserWarning):
    """
    Warnings produced by Cate's data stores and data sources instances, used to report any problems handling data.
    """
    pass


class DataAccessError(Exception):
    """
    Exceptions produced by Cate's data stores and data sources instances,
    used to report any problems handling data.
    """


class NetworkError(ConnectionError):
    """
    Exceptions produced by Cate's data stores and data sources instances,
    used to report any problems with the network or in case an endpoint
    couldn't be found nor reached.
    """
    pass


class DataStoreNotice:
    """
    A short notice that can be exposed to users by data stores.
    """

    def __init__(self, id: str, title: str, content: str, intent: str = None, icon: str = None):
        """
        A short notice that can be exposed to users by data stores.

        :param id: Notice ID.
        :param title: A human-readable, plain text title.
        :param content: A human-readable, plain text title that may be formatted using Markdown.
        :param intent: Notice intent, may be one of "default", "primary", "success", "warning", "danger"
        :param icon: An option icon name. See https://blueprintjs.com/docs/versions/1/#core/icons
        """
        if id is None or id == "":
            raise ValueError("invalid id")
        if title is None or title == "":
            raise ValueError("invalid title")
        if content is None or content == "":
            raise ValueError("invalid content")
        if intent not in {None, "default", "primary", "success", "warning", "danger"}:
            raise ValueError("invalid intent")

        self._dict = dict(id=id, title=title, content=content, icon=icon, intent=intent)

    @property
    def id(self):
        return self._dict["id"]

    @property
    def title(self):
        return self._dict["title"]

    @property
    def content(self):
        return self._dict["content"]

    @property
    def intent(self):
        return self._dict["intent"]

    @property
    def icon(self):
        return self._dict["icon"]

    def to_dict(self):
        return dict(self._dict)


class DataStore(metaclass=ABCMeta):
    """
    Represents a data store of data sources.

    :param ds_id: Unique data store identifier.
    :param title: A human-readable tile.
    """

    def __init__(self, ds_id: str, title: str = None, is_local: bool = False):
        self._id = ds_id
        self._title = title or ds_id
        self._is_local = is_local

    @property
    def id(self) -> str:
        """
        Return the unique identifier for this data store.
        """
        return self._id

    @property
    def title(self) -> str:
        """
        Return a human-readable tile for this data store.
        """
        return self._title

    @property
    def description(self) -> Optional[str]:
        """
        Return an optional, human-readable description for this data store as plain text.

        The text may use Markdown formatting.
        """
        return None

    @property
    def notices(self) -> List[DataStoreNotice]:
        """
        Return an optional list of notices for this data store that can be used to inform users about the
        conventions, standards, and data extent used in this data store or upcoming service outages.
        """
        return []

    @property
    def is_local(self) -> bool:
        """
        Whether this is a remote data source not requiring any internet connection when its ``query()`` method
        is called or the ``open_dataset()`` and ``make_local()`` methods on one of its data sources.
        """
        return self._is_local

    @abstractmethod
    def get_data_ids(self) -> Sequence[Tuple[str, Optional[str]]]:
        """
        Get a sequence of the data resource identifiers provided by this data store.

        The returned sequence items are 2-tuples of the form (*data_id*, *title*), where *data_id*
        is the actual data identifier and *title* is an optional, human-readable title for the data.

        :return: A sequence of the identifiers and titles of data resources provided by this data store.
        :raise DataStoreError: If an error occurs.
        """

    def has_data(self, data_id: str) -> bool:
        data_ids = [did[0] for did in self.get_data_ids()]
        return data_id in data_ids

    @abstractmethod
    def describe_data(self, data_id: str) -> xcube_store.DataDescriptor:
        """
        Get the descriptor for the data resource given by *data_id*.

        Raises a DataStoreError if *data_id* does not exist in this store.

        :return a data-type specific data descriptor
        :raise DataStoreError: If an error occurs.
        """

    @abstractmethod
    def open_data(self,
                  data_id: str,
                  **open_params) -> Any:
        """
        Open the data given by the data resource identifier *data_id* using the supplied *open_params*.

        The data type of the return value depends on the data opener used to open the data resource.

        *open_params* must comply with the schema of the opener's parameters.

        Raises if *data_id* does not exist in this store.

        :param data_id: The data identifier that is known to exist in this data store.
        :param open_params: Opener-specific parameters.
        :return: An in-memory representation of the data resources identified by *data_id* and *open_params*.
        :raise DataStoreError: If an error occurs.
        """

    @abstractmethod
    def write_data(self,
                   data: Any,
                   data_id: str = None) -> str:
        """
        Write a data in-memory instance using the supplied *data_id*.

        If data identifier *data_id* is not given, a writer-specific default will be generated, used, and returned.

        Raises a DataStoreError if *data_id* does not exist in this store or if this store does not support writing.

        :param data: The data in-memory instance to be written.
        :param data_id: An optional data identifier that is known to be unique in this data store.
        :return: The data identifier used to write the data.
        :raise DataStoreError: If an error occurs.
        """

    def invalidate(self):
        """
        Datastore might use a cached list of available dataset which can change in time.
        Resources managed by a datastore are external so we have to consider that they can
        be updated by other process.
        This method ask to invalidate the internal structure and synchronize it with the
        current status
        :return:
        """
        pass

    def get_updates(self, reset=False) -> Dict:
        """
        Ask the datastore to retrieve the differences found between a previous
        dataStore status and the current one,
        The implementation return a dictionary with the new ['new'] and removed ['del'] dataset.
        it also return the reference time to the datastore status taken as previous.
        Reset flag is used to clean up the support files, freeze and diff.
        :param: reset=False. Set this flag to true to clean up all the support files forcing a
                synchronization with the remote catalog
        :return: A dictionary with keys { 'generated', 'source_ref_time', 'new', 'del' }.
                 genetated: generation time, when the check has been executed
                 source_ref_time: when the local copy of the remoted dataset hes been made.
                                  It is also used by the system to refresh the current images when
                                  is older then 1 day.
                 new: a list of new dataset entry
                 del: a list of removed datset
        """
        generated = datetime.datetime.now()
        report = {"generated": str(generated),
                  "source_ref_time": str(generated),
                  "new": list(),
                  "del": list()}
        return report

    def _repr_html_(self):
        """Provide an HTML representation of this object for IPython."""
        rows = []
        row_count = 0
        for data_id in self.get_data_ids():
            row_count += 1
            # noinspection PyProtectedMember
            rows.append('<tr><td><strong>%s</strong></td><td>%s</td></tr>' % (row_count, data_id[0]))
        return '<p>Contents of %s</p><table>%s</table>' % (self.id, '\n'.join(rows))


class XcubeDataStore(DataStore):

    def __init__(self,
                 store_config: dict,
                 ds_id: str,
                 index_cache_used: bool = True,
                 index_cache_expiration_days: float = 1.0,
                 meta_data_store_path: str = None):

        store_id = store_config.get('store_id', '')

        def predicate(extension: xcube_extension.Extension):
            return store_id == extension.name

        extensions = xcube_store.find_data_store_extensions(predicate=predicate)
        if not extensions:
            raise ValueError(f'Could not find data store {store_id}')

        store_extension = extensions[0]

        notices = store_extension.metadata.get('notices', [])
        self._data_store_notices = []
        for notice in notices:
            self._data_store_notices.append(DataStoreNotice(notice[0], notice[1], notice[2], notice[3], notice[4]))
        self._description = store_extension.metadata.get('description', ''),
        super().__init__(ds_id,
                         store_extension.metadata.get('title', ''),
                         store_extension.metadata.get('is_local', False)
                         )
        self._store_config = store_config
        self._store = None
        self._index_cache_used = index_cache_used
        self._index_cache_expiration_days = index_cache_expiration_days
        if meta_data_store_path:
            self._metadata_store_path = meta_data_store_path
        else:
            self._metadata_store_path = os.environ.get('CATE_ESA_CCI_ODP_DATA_STORE_PATH',
                                                       os.path.join(get_data_stores_path(), self.id))

    # noinspection PyTypeChecker
    @property
    def description(self) -> Optional[str]:
        return self._description

    @property
    def notices(self) -> List[DataStoreNotice]:
        return self._data_store_notices

    def _get_store(self):
        if not self._store:
            store_params = self._store_config.get('store_params', {})
            self._store = xcube_store.new_data_store(
                data_store_id=self._store_config.get('store_id', ''),
                extension_registry=None,
                **store_params
            )
        return self._store

    @staticmethod
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

            timestamp = datetime.datetime(year=2000, month=1, day=1)
            if os.path.exists(cache_timestamp_file):
                with open(cache_timestamp_file) as fp:
                    timestamp_text = fp.read()
                    timestamp = datetime.datetime.strptime(timestamp_text, _TIMESTAMP_FORMAT)

            time_diff = datetime.datetime.now() - timestamp
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
                if cache_used and len(json_obj) > 0:
                    os.makedirs(cache_dir, exist_ok=True)
                    # noinspection PyUnboundLocalVariable
                    with open(cache_json_file, "w") as fp:
                        fp.write(json.dumps(json_obj, indent='  '))
                    # noinspection PyUnboundLocalVariable
                    with open(cache_timestamp_file, "w") as fp:
                        fp.write(datetime.datetime.utcnow().strftime(_TIMESTAMP_FORMAT))
            except Exception as e:
                if cache_json_file and os.path.exists(cache_json_file):
                    with open(cache_json_file) as fp:
                        json_text = fp.read()
                        json_obj = json.loads(json_text)
                else:
                    raise e

        return json_obj

    def get_data_ids(self) -> Sequence[Tuple[str, Optional[str]]]:
        return self._load_or_fetch_json(self._get_data_ids,
                                        cache_used=self._index_cache_used,
                                        cache_dir=self._metadata_store_path,
                                        cache_json_filename='data-ids.json',
                                        cache_timestamp_filename='data-ids-timestamp.txt',
                                        cache_expiration_days=self._index_cache_expiration_days
                                        )

    def _get_data_ids(self) -> List[Tuple[str, Optional[str]]]:
        store = self._get_store()
        return list(store.get_data_ids())

    # TODO xcube integ.: make this generic, e.g. class method DataDescriptor.from_dict()
    def describe_data(self, data_id: str) -> xcube_store.DataDescriptor:
        descriptor_dict = self._load_or_fetch_json(self._describe_data,
                                                   fetch_json_args=[data_id],
                                                   cache_used=self._index_cache_used,
                                                   cache_dir=self._metadata_store_path,
                                                   cache_json_filename=f'{data_id}.json',
                                                   cache_timestamp_filename=f'{data_id}-timestamp.txt',
                                                   cache_expiration_days=self._index_cache_expiration_days)
        type_specifier = descriptor_dict.get('type_specifier')
        if type_specifier is not None:
            if xcube_store.TYPE_SPECIFIER_MULTILEVEL_DATASET.is_satisfied_by(type_specifier):
                return xcube_store.MultiLevelDatasetDescriptor.from_dict(descriptor_dict)
            if xcube_store.TYPE_SPECIFIER_DATASET.is_satisfied_by(type_specifier):
                return xcube_store.DatasetDescriptor.from_dict(descriptor_dict)
            if xcube_store.TYPE_SPECIFIER_GEODATAFRAME.is_satisfied_by(type_specifier):
                return xcube_store.GeoDataFrameDescriptor.from_dict(descriptor_dict)
        return xcube_store.DataDescriptor.from_dict(descriptor_dict)

    def _describe_data(self, data_id: str) -> Dict:
        store = self._get_store()
        return store.describe_data(data_id).to_dict()

    def has_data(self, data_id: str) -> bool:
        data_ids = [did[0] for did in self.get_data_ids()]
        return data_id in data_ids

    def open_data(self, data_id: str, **open_params):
        store = self._get_store()
        #todo exchange this code when we have decided how to deal with type specifiers
        if 'dataset[cube]' in store.get_type_specifiers_for_data(data_id):
            cube_opener_id = store.get_data_opener_ids(data_id=data_id,
                                                       type_specifier='dataset[cube]')[0]
            if cube_opener_id:
                return store.open_data(data_id, opener_id=cube_opener_id, **open_params)
        return store.open_data(data_id, **open_params)

    def write_data(self, data: Any, data_id: str = None) -> str:
        store = self._get_store()
        if not isinstance(store, xcube_store.MutableDataStore):
            raise xcube_store.DataStoreError(f'Store {self.id} is not mutable. No data can be written to this store.')
        return store.write_data(data=data,
                                data_id=data_id)


class DataStoreRegistry:
    """
    Registry of :py:class:`DataStore` objects.
    """

    def __init__(self):
        self._data_stores = dict()

    def get_data_store(self, ds_id: str) -> Optional[DataStore]:
        return self._data_stores.get(ds_id)

    def get_data_stores(self) -> Sequence[DataStore]:
        return list(self._data_stores.values())

    def add_data_store(self, data_store: DataStore):
        self._data_stores[data_store.id] = data_store

    def remove_data_store(self, ds_id: str):
        del self._data_stores[ds_id]

    def __len__(self):
        return len(self._data_stores)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        import pprint
        return pprint.pformat(self._data_stores)

    def _repr_html_(self):
        rows = []
        for ds_id, data_store in self._data_stores.items():
            rows.append('<tr><td>%s</td><td>%s</td></tr>' % (ds_id, repr(data_store)))
        return '<table>%s</table>' % '\n'.join(rows)


#: The data data store registry of type :py:class:`DataStoreRegistry`.
#: Use it add new data stores to Cate.
DATA_STORE_REGISTRY = DataStoreRegistry()

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


def get_metadata_from_descriptor(descriptor: xcube_store.DataDescriptor) -> Dict:
    metadata = dict(data_id=descriptor.data_id,
                    type_specifier=descriptor.type_specifier)
    if descriptor.crs:
        metadata['crs'] = descriptor.crs
    if descriptor.bbox:
        metadata['bbox'] = descriptor.bbox
    if descriptor.spatial_res:
        metadata['spatial_res'] = descriptor.spatial_res
    if descriptor.time_range:
        metadata['time_range'] = descriptor.time_range
    if descriptor.time_period:
        metadata['time_period'] = descriptor.time_period
    if hasattr(descriptor, 'attrs'):
        for name in INFO_FIELD_NAMES:
            value = descriptor.attrs.get(name, None)
            # Many values are one-element lists: turn them into scalars
            if isinstance(value, list) and len(value) == 1:
                value = value[0]
            if value is not None:
                metadata[name] = value
    if hasattr(descriptor, 'data_vars'):
        metadata['variables'] = []
        for var_descriptor in descriptor.data_vars:
            var_dict = dict(name=var_descriptor.name)
            if var_descriptor.attrs:
                var_dict['units'] = var_descriptor.attrs.get('units', '')
                var_dict['long_name'] = var_descriptor.attrs.get('long_name', '')
                var_dict['standard_name'] = var_descriptor.attrs.get('standard_name', '')
            metadata['variables'].append(var_dict)
    return metadata


def get_info_string_from_data_descriptor(descriptor: xcube_store.DataDescriptor) -> str:
    meta_info = get_metadata_from_descriptor(descriptor)

    max_len = 0
    for name in meta_info.keys():
        max_len = max(max_len, len(name))

    info_lines = []
    for name, value in meta_info.items():
        if name != 'variables':
            info_lines.append('%s:%s %s' % (name, (1 + max_len - len(name)) * ' ', value))

    return '\n'.join(info_lines)


def find_data_store(ds_id: str, data_stores: Union[DataStore, Sequence[DataStore]] = None) -> Optional[DataStore]:
    """
    Find the data store that includes the given *ds_id*.
    This will raise an exception if the *ds_id* is given in more than one data store.

    :param ds_id:  A data source identifier.
    :param data_stores: If given these data stores will be queried. Otherwise all registered data stores will be used.
    :return: All data sources matching the given constrains.
    """
    results = []
    if data_stores is None:
        data_store_list = DATA_STORE_REGISTRY.get_data_stores()
    else:
        data_store_list = data_stores
    for data_store in data_store_list:
        if data_store.has_data(ds_id):
            results.append(data_store)
    if len(results) > 1:
        raise ValidationError(f'{len(results)} data sources found for the given ID {ds_id!r}')
    if len(results) == 1:
        return results[0]


def get_data_descriptor(ds_id: str) -> Optional[xcube_store.DataDescriptor]:
    data_store = find_data_store(ds_id)
    if data_store:
        return data_store.describe_data(ds_id)


def open_dataset(dataset_id: str,
                 time_range: TimeRangeLike.TYPE = None,
                 region: PolygonLike.TYPE = None,
                 var_names: VarNamesLike.TYPE = None,
                 force_local: bool = False,
                 local_ds_id: str = None,
                 monitor: Monitor = Monitor.NONE) -> Tuple[Any, str]:
    """
    Open a dataset from a data source.

    :param dataset_id: The identifier of an ECV dataset. Must not be empty.
    :param time_range: An optional time constraint comprising start and end date.
           If given, it must be a :py:class:`TimeRangeLike`.
    :param region: An optional region constraint.
           If given, it must be a :py:class:`PolygonLike`.
    :param var_names: Optional names of variables to be included.
           If given, it must be a :py:class:`VarNamesLike`.
    :param force_local: Optional flag for remote data sources only
           Whether to make a local copy of data source if it's not present
    :param local_ds_id: Optional, fpr remote data sources only
           Local data source ID for newly created copy of remote data source
    :param monitor: A progress monitor
    :return: A tuple consisting of a new dataset instance and its id
    """
    if not dataset_id:
        raise ValidationError('No data source given')

    data_store = find_data_store(ds_id=dataset_id)
    if not data_store:
        raise ValidationError(f"No data store found that contains the ID '{dataset_id}'")

    args = {}
    if var_names:
        args['var_names'] = VarNamesLike.convert(var_names)
    if time_range:
        time_range = TimeRangeLike.convert(time_range)
        args['time_range'] = [datetime.datetime.strftime(time_range[0], '%Y-%m-%d'),
                              datetime.datetime.strftime(time_range[1], '%Y-%m-%d')]
    if region:
        args['bbox'] = list(PolygonLike.convert(region).bounds)

    dataset = data_store.open_data(data_id=dataset_id,
                                   **args)
    if force_local:
        dataset, dataset_id = make_local(data=dataset,
                                         local_name=local_ds_id)
    return dataset, dataset_id


def make_local(data: Any,
               local_name: Optional[str] = None
               ) -> Optional[Tuple[xr.Dataset, str]]:
    local_store = DATA_STORE_REGISTRY.get_data_store('local')
    if not local_store:
        raise ValueError('Cannot initialize `local` DataStore')

    local_data_id = local_store.write_data(data=data, data_id=local_name)
    return local_store.open_data(data_id=local_data_id), local_data_id


# noinspection PyUnresolvedReferences,PyProtectedMember
def open_xarray_dataset(paths,
                        region: PolygonLike.TYPE = None,
                        var_names: VarNamesLike.TYPE = None,
                        monitor: Monitor = Monitor.NONE,
                        **kwargs) -> xr.Dataset:
    r"""
    Open multiple files as a single dataset. This uses dask. If each individual file
    of the dataset is small, one Dask chunk will coincide with one temporal slice,
    e.g. the whole array in the file. Otherwise smaller dask chunks will be used
    to split the dataset.

    :param paths: Either a string glob in the form "path/to/my/files/\*.nc" or an explicit
        list of files to open.
    :param region: Optional region constraint.
    :param var_names: Optional variable names constraint.
    :param monitor: Optional progress monitor.
    :param kwargs: Keyword arguments directly passed to ``xarray.open_mfdataset()``
    """
    # paths could be a string or a list
    files = []
    if isinstance(paths, str):
        files.append(paths)
    else:
        files.extend(paths)

    # should be a file or a glob or an URL
    files = [(i,) if re.match(URL_REGEX, i) else glob.glob(i) for i in files]
    # make a flat list
    files = list(itertools.chain.from_iterable(files))

    if not files:
        raise IOError('File {} not found'.format(paths))

    if 'concat_dim' in kwargs:
        concat_dim = kwargs.pop('concat_dim')
    else:
        concat_dim = 'time'

    if 'chunks' in kwargs:
        chunks = kwargs.pop('chunks')
    elif len(files) > 1:
        # By default the dask chunk size of xr.open_mfdataset is (1, lat, lon). E.g.,
        # the whole array is one dask slice irrespective of chunking on disk.
        #
        # netCDF files can also feature a significant level of compression rendering
        # the known file size on disk useless to determine if the default dask chunk
        # will be small enough that a few of them could comfortably fit in memory for
        # parallel processing.
        #
        # Hence we open the first file of the dataset and detect the maximum chunk sizes
        # used in the spatial dimensions.
        #
        # If no such sizes could be found, we use xarray's default chunking.
        chunks = get_spatial_ext_chunk_sizes(files[0])
    else:
        chunks = None

    def preprocess(raw_ds: xr.Dataset):
        # Add a time dimension if attributes "time_coverage_start" and "time_coverage_end" are found.
        norm_ds = normalize_missing_time(normalize_coord_vars(raw_ds))
        monitor.progress(work=1)
        return norm_ds

    with monitor.starting('Opening dataset', len(files)):
        # autoclose ensures that we can open datasets consisting of a number of
        # files that exceeds OS open file limit.
        # TODO (Suvi): - might need 2 versions ( one with combine=nested, coords=concat_dim and
        #  other with combine='by_coords' to support opening most datasets
        #  - we can eleminate unnecessary preprocess on datasets that already have time coordinate,
        #  this can be when creating datasource.
        #  - enable parallel=True to open files parallely to preprocess only when number of files
        #  are more, otherwise it is determental to perforamnce.

        ds = xr.open_mfdataset(files,
                               coords='minimal',
                               chunks=chunks,
                               preprocess=preprocess,
                               # Future behaviour will be
                               combine='by_coords',
                               # combine='nested',
                               # parallel=True,
                               compat='override',
                               **kwargs)

    if var_names:
        ds = ds.drop_vars([var_name for var_name in ds.data_vars.keys() if var_name not in var_names])

    ds = normalize_impl(ds)

    if region:
        ds = subset_spatial_impl(ds, region)

    return ds


def get_spatial_ext_chunk_sizes(ds_or_path: Union[xr.Dataset, str]) -> Dict[str, int]:
    """
    Get the spatial, external chunk sizes for the latitude and longitude dimensions
    of a dataset as provided in a variable's encoding object.

    :param ds_or_path: An xarray dataset or a path to file that can be opened by xarray.
    :return: A mapping from dimension name to external chunk sizes.
    """
    if isinstance(ds_or_path, str):
        ds = xr.open_dataset(ds_or_path, decode_times=False)
    else:
        ds = ds_or_path
    lon_name = get_lon_dim_name(ds)
    lat_name = get_lat_dim_name(ds)
    if lon_name and lat_name:
        chunk_sizes = get_ext_chunk_sizes(ds, {lat_name, lon_name})
    else:
        chunk_sizes = None
    if isinstance(ds_or_path, str):
        ds.close()
    return chunk_sizes


def get_ext_chunk_sizes(ds: xr.Dataset, dim_names: Set[str] = None,
                        init_value=0, map_fn=max, reduce_fn=None) -> Dict[str, int]:
    """
    Get the external chunk sizes for each dimension of a dataset as provided in a variable's encoding object.

    :param ds: The dataset.
    :param dim_names: The names of dimensions of data variables whose external chunking should be collected.
    :param init_value: The initial value (not necessarily a chunk size) for mapping multiple different chunk sizes.
    :param map_fn: The mapper function that maps a chunk size from a previous (initial) value.
    :param reduce_fn: The reducer function the reduces multiple mapped chunk sizes to a single one.
    :return: A mapping from dimension name to external chunk sizes.
    """
    agg_chunk_sizes = None
    for var_name in ds.variables:
        var = ds[var_name]
        if var.encoding:
            chunk_sizes = var.encoding.get('chunksizes')
            if chunk_sizes \
                    and len(chunk_sizes) == len(var.dims) \
                    and (not dim_names or dim_names.issubset(set(var.dims))):
                for dim_name, size in zip(var.dims, chunk_sizes):
                    if not dim_names or dim_name in dim_names:
                        if agg_chunk_sizes is None:
                            agg_chunk_sizes = dict()
                        old_value = agg_chunk_sizes.get(dim_name)
                        agg_chunk_sizes[dim_name] = map_fn(size, init_value if old_value is None else old_value)
    if agg_chunk_sizes and reduce_fn:
        agg_chunk_sizes = {k: reduce_fn(v) for k, v in agg_chunk_sizes.items()}
    return agg_chunk_sizes


def format_variables_info_string(descriptor: xcube_store.DataDescriptor):
    """
    Return some textual information about the variables described by this DataDescriptor.
    Useful for CLI / REPL applications.
    :param variables:
    :return:
    """
    meta_info = get_metadata_from_descriptor(descriptor)
    variables = meta_info.get('variables', [])
    if len(variables) == 0:
        return 'No variables information available.'

    info_lines = []
    for variable in variables:
        info_lines.append('%s (%s):' % (variable.get('name', '?'), variable.get('units', '-')))
        info_lines.append('  Long name:        %s' % variable.get('long_name', '?'))
        info_lines.append('  CF standard name: %s' % variable.get('standard_name', '?'))
        info_lines.append('')

    return '\n'.join(info_lines)


def format_cached_datasets_coverage_string(cache_coverage: dict) -> str:
    """
    Return a textual representation of information about cached, locally available data sets.
    Useful for CLI / REPL applications.
    :param cache_coverage:
    :return:
    """
    if not cache_coverage:
        return 'No information about cached datasets available.'

    info_lines = []
    for date_from, date_to in sorted(cache_coverage.items()):
        info_lines.append('{date_from} to {date_to}'
                          .format(date_from=date_from.strftime('%Y-%m-%d'),
                                  date_to=date_to.strftime('%Y-%m-%d')))

    return '\n'.join(info_lines)
