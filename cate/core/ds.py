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
import logging
import re
from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Sequence, Optional, Union, Any, Dict, Set, List, Tuple, Iterator

import xarray as xr

import xcube.core.store as xcube_store
import xcube.util.extension as xcube_extension

from .cdm import Schema, get_lon_dim_name, get_lat_dim_name
from .opimpl import normalize_missing_time, normalize_coord_vars, normalize_impl, subset_spatial_impl
from .types import PolygonLike, TimeRange, TimeRangeLike, VarNamesLike, ValidationError
from ..util.monitor import Monitor

__author__ = "Norman Fomferra (Brockmann Consult GmbH), " \
             "Marco Zühlke (Brockmann Consult GmbH), " \
             "Chris Bernat (Telespazio VEGA UK Ltd)"

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


class DataSource(metaclass=ABCMeta):
    """
    An abstract data source from which datasets can be retrieved.
    """

    @property
    @abstractmethod
    def id(self) -> str:
        """Data source identifier."""

    # TODO (forman): issue #399 - remove it, no use
    @property
    def schema(self) -> Optional[Schema]:
        """
        The data :py:class:`Schema` for any dataset provided by this data source or ``None`` if unknown.
        Currently unused in cate.
        """
        return None

    # TODO (forman): issue #399 - make this a property or call it "get_temporal_coverage(...)"
    def temporal_coverage(self, monitor: Monitor = Monitor.NONE) -> Optional[TimeRange]:
        """
        The temporal coverage as tuple (*start*, *end*) where *start* and *end* are UTC ``datetime`` instances.

        :param monitor: a progress monitor.
        :return: A tuple of (*start*, *end*) UTC ``datetime`` instances or ``None`` if the temporal coverage is unknown.
        """
        return None

    @property
    @abstractmethod
    def data_store(self) -> 'DataStore':
        """The data store to which this data source belongs."""

    @property
    def status(self) -> 'DataSourceStatus':
        """
        Return information about data source accessibility
        """
        return DataSourceStatus.READY

    # TODO (forman): issue #399 - remove "ds_id", see TODO on "DataStore.query()"
    def matches(self, ds_id: str = None, query_expr: str = None) -> bool:
        """
        Test if this data source matches the given *id* or *query_expr*.
        If neither *id* nor *query_expr* are given, the method returns True.

        :param ds_id: A data source identifier.
        :param query_expr: A query expression. Currently, only simple search strings are supported.
        :return: True, if this data sources matches the given *id* or *query_expr*.
        """
        if ds_id and ds_id.lower() == self.id.lower():
            return True
        if query_expr:
            if query_expr.lower() in self.id.lower():
                return True
            if self.title and query_expr.lower() in self.title.lower():
                return True
        return False

    @abstractmethod
    def open_dataset(self,
                     time_range: TimeRangeLike.TYPE = None,
                     region: PolygonLike.TYPE = None,
                     var_names: VarNamesLike.TYPE = None,
                     protocol: str = None,
                     monitor: Monitor = Monitor.NONE) -> Any:
        """
        Open a dataset from this data source.

        :param time_range: An optional time constraint comprising start and end date.
                If given, it must be a :py:class:`TimeRangeLike`.
        :param region: An optional region constraint.
                If given, it must be a :py:class:`PolygonLike`.
        :param var_names: Optional names of variables to be included.
                If given, it must be a :py:class:`VarNamesLike`.
        :param protocol: **Deprecated.** Protocol name, if None selected default protocol
                will be used to access data.
        :param monitor: A progress monitor.
        :return: A dataset instance or ``None`` if no data is available for the given constraints.
        """

    @abstractmethod
    def make_local(self,
                   local_name: str,
                   local_id: str = None,
                   time_range: TimeRangeLike.TYPE = None,
                   region: PolygonLike.TYPE = None,
                   var_names: VarNamesLike.TYPE = None,
                   monitor: Monitor = Monitor.NONE) -> Optional['DataSource']:
        """
        Turns this (likely remote) data source into a local data source given a name and a number of
        optional constraints.

        If this is a remote data source, data will be downloaded and turned into a local data source which will
        be added to the data store named "local".

        If this is already a local data source, a new local data source will be created by copying
        required data or data subsets.

        The method returns the newly create local data source.

        :param local_name: A human readable name for the new local data source.
        :param local_id: A unique ID to be used for the new local data source.
               If not given, a new ID will be generated.
        :param time_range: An optional time constraint comprising start and end date.
               If given, it must be a :py:class:`TimeRangeLike`.
        :param region: An optional region constraint.
               If given, it must be a :py:class:`PolygonLike`.
        :param var_names: Optional names of variables to be included.
               If given, it must be a :py:class:`VarNamesLike`.
        :param monitor: A progress monitor.
        :return: the new local data source
        """
        pass

    @property
    def title(self) -> Optional[str]:
        """
        Human-readable data source title.
        The default implementation tries to retrieve the title from ``meta_info['title']``.
        """
        meta_info = self.meta_info
        if meta_info is None:
            return None
        return meta_info.get('title')

    # TODO (forman): issue #399 - explain expected metadata entries and their formats, e.g."variables"
    @property
    def meta_info(self) -> Optional[dict]:
        """
        Return meta-information about this data source.
        The returned dict, if any, is JSON-serializable.
        """
        return None

    @property
    def cache_info(self) -> Optional[dict]:
        """
        Return information about cached, locally available data sets.
        The returned dict, if any, is JSON-serializable.
        """
        return None

    @property
    def variables_info(self) -> Optional[dict]:
        """
        Return meta-information about the variables contained in this data source.
        The returned dict, if any, is JSON-serializable.
        """
        return None

    @property
    def info_string(self) -> str:
        """
        Return a textual representation of the meta-information about this data source.
        Useful for CLI / REPL applications.
        """
        meta_info = self.meta_info

        if not meta_info:
            return 'No data source meta-information available.'

        max_len = 0
        for name in meta_info.keys():
            max_len = max(max_len, len(name))

        info_lines = []
        for name, value in meta_info.items():
            if name != 'variables':
                info_lines.append('%s:%s %s' % (name, (1 + max_len - len(name)) * ' ', value))

        return '\n'.join(info_lines)

    def __str__(self):
        return self.info_string

    # TODO (forman): issue #399 - remove @abstractmethod, provide reasonable default impl. to make it a convenient ABC
    @abstractmethod
    def _repr_html_(self):
        """Provide an HTML representation of this object for IPython."""

    def _cannot_access_error(self, time_range=None, region=None, var_names=None,
                             verb="open", cause: BaseException = None, error_cls=DataAccessError):
        error_message = f'Failed to {verb} data source "{self.id}"'
        constraints = []
        if time_range is not None and time_range != "":
            constraints.append("time range")
        if region is not None and region != "":
            constraints.append("region")
        if var_names is not None and var_names != "":
            constraints.append("variable names")
        if constraints:
            error_message += " for given " + ", ".join(constraints)
        if cause is not None:
            error_message += f": {cause}"
        _LOG.info(error_message)
        return error_cls(error_message)

    def _empty_error(self, time_range=None):
        error_message = f'Data source "{self.id}" does not seem to have any datasets'
        if time_range is not None:
            error_message += f' in given time range {TimeRangeLike.format(time_range)}'
        _LOG.info(error_message)
        return DataAccessError(error_message)


class DataSourceStatus(Enum):
    """
    Enum stating current state of Data Source accessibility.
     * READY - data is complete and ready to use
     * ERROR - data initialization process has been interrupted, causing that data source is incomplete or/and corrupted
     * PROCESSING - data source initialization process is in progress.
     * CANCELLED - data initialization process has been intentionally interrupted by user
    """
    READY = "READY",
    ERROR = "ERROR",
    PROCESSING = "PROCESSING",
    CANCELLED = "CANCELLED"


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

    # TODO (forman): issue #399 - introduce get_data_source(ds_id), we have many usages in code, ALT+F7 on "query"
    # @abstractmethod
    # def get_data_source(self, ds_id: str, monitor: Monitor = Monitor.NONE) -> Optional[DataSource]:
    #     """
    #     Get data sources by identifier *ds_id*.
    #
    #     :param ds_id: Data source identifier.
    #     :param monitor:  A progress monitor.
    #     :return: The data sources, or ``None`` if it doesn't exists.
    #     """

    # TODO (forman): issue #399 - remove "ds_id" keyword, use "get_data_source(ds_id)" instead
    # TODO (forman): issue #399 - code duplication: almost all implementations are same or very similar
    @abstractmethod
    def query(self, ds_id: str = None, query_expr: str = None, monitor: Monitor = Monitor.NONE) -> Sequence[DataSource]:
        """
        Retrieve data sources in this data store using the given constraints.

        :param ds_id: Data source identifier.
        :param query_expr: Query expression which may be used if *ìd* is unknown.
        :param monitor:  A progress monitor.
        :return: Sequence of data sources.
        """

    # TODO (forman): issue #399 - remove @abstractmethod, provide reasonable default impl. to make it a convenient ABC
    @abstractmethod
    def _repr_html_(self):
        """Provide an HTML representation of this object for IPython."""


class XcubeDataStore(DataStore):

    def __init__(self, store_config: dict, ds_id: str):

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

    def get_data_ids(self) -> Iterator[Tuple[str, Optional[str]]]:
        store = self._get_store()
        return store.get_data_ids()

    def describe_data(self, data_id: str) -> xcube_store.DataDescriptor:
        store = self._get_store()
        return store.describe_data(data_id)

    def query(self, ds_id: str = None, query_expr: str = None, monitor: Monitor = Monitor.NONE) -> Sequence[DataSource]:
        #TODO remove
        pass

    def _repr_html_(self):
        return self.id


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


def find_data_sources_update(data_stores: Union[DataStore, Sequence[DataStore]] = None) -> Dict:
    """
    find difference in the list of data source of the given data store (all when None).
    The updateds will be returned as dictionaty where the key is the Data store ID.
    The value is a dictionary too contining the list of 'new', 'de' (removed) dataset
    :param data_stores: list of Data store(s) to be cheked. If None all the refgistered Data store
                        will be checked
    :return: dictionary index by data store ID, values are a second dictionary with the updates sorted by
             new and del data source in addition to source_ref_time which is the time of snapshot used to
             compare the data source list
    """
    data_store_list = []
    if data_stores is None:
        data_store_list = DATA_STORE_REGISTRY.get_data_stores()
    response = dict()

    for ds in data_store_list:
        r = ds.get_updates()
        if r['new'] or r['del']:
            response[ds.id] = r

    return response


def find_data_sources(data_stores: Union[DataStore, Sequence[DataStore]] = None,
                      ds_id: str = None,
                      query_expr: str = None) -> Sequence[DataSource]:
    """
    Find data sources in the given data store(s) matching the given *id* or *query_expr*.

    See also :py:func:`open_dataset`.

    :param data_stores: If given these data stores will be queried. Otherwise all registered data stores will be used.
    :param ds_id:  A data source identifier.
    :param query_expr:  A query expression.
    :return: All data sources matching the given constrains.
    """
    results = []
    primary_data_store = None
    data_store_list = []
    if data_stores is None:
        data_store_list = DATA_STORE_REGISTRY.get_data_stores()
    elif isinstance(data_stores, DataStore):
        primary_data_store = data_stores
    else:
        data_store_list = data_stores

    for data_store in data_store_list:
        # datastore cache might be out of synch
        data_store.invalidate()

    if not primary_data_store and ds_id and ds_id.count('.') > 0:
        primary_data_store_index = -1
        primary_data_store_id, data_source_name = ds_id.split('.', 1)
        for idx, data_store in enumerate(data_store_list):
            if data_store.id == primary_data_store_id:
                primary_data_store_index = idx
        if primary_data_store_index >= 0:
            primary_data_store = data_store_list.pop(primary_data_store_index)

    if primary_data_store:
        results.extend(primary_data_store.query(ds_id=ds_id, query_expr=query_expr))
    if not results:
        # noinspection PyTypeChecker
        for data_store in data_store_list:
            results.extend(data_store.query(ds_id=ds_id, query_expr=query_expr))
    return results


def open_dataset(data_source: Union[DataSource, str],
                 time_range: TimeRangeLike.TYPE = None,
                 region: PolygonLike.TYPE = None,
                 var_names: VarNamesLike.TYPE = None,
                 force_local: bool = False,
                 local_ds_id: str = None,
                 monitor: Monitor = Monitor.NONE) -> Any:
    """
    Open a dataset from a data source.

    :param data_source: A ``DataSource`` object or a string.
           Strings are interpreted as the identifier of an ECV dataset and must not be empty.
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
    :return: An new dataset instance
    """
    if not data_source:
        raise ValidationError('No data source given')

    if isinstance(data_source, str):
        data_store_list = list(DATA_STORE_REGISTRY.get_data_stores())
        data_sources = find_data_sources(data_store_list, ds_id=data_source)
        if len(data_sources) == 0:
            raise ValidationError(f'No data sources found for the given ID {data_source!r}')
        elif len(data_sources) > 1:
            raise ValidationError(f'{len(data_sources)} data sources found for the given ID {data_source!r}')
        data_source = data_sources[0]

    if force_local:
        with monitor.starting('Opening dataset', 100):
            data_source = data_source.make_local(local_name=local_ds_id if local_ds_id else "",
                                                 time_range=time_range, region=region, var_names=var_names,
                                                 monitor=monitor.child(80))
            return data_source.open_dataset(time_range, region, var_names, monitor=monitor.child(20))
    else:
        return data_source.open_dataset(time_range, region, var_names, monitor=monitor)


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
                               #combine='nested',
                               #parallel=True,
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


def format_variables_info_string(variables: dict):
    """
    Return some textual information about the variables contained in this data source.
    Useful for CLI / REPL applications.
    :param variables:
    :return:
    """
    if not variables:
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
