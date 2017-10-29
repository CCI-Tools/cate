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

import glob
from abc import ABCMeta, abstractmethod
from enum import Enum
from math import ceil, sqrt
from typing import Sequence, Optional, Union, Any

import xarray as xr

from .cdm import Schema, get_lon_dim_name, get_lat_dim_name
from .types import PolygonLike, TimeRange, TimeRangeLike, VarNamesLike
from ..util.monitor import Monitor

__author__ = "Norman Fomferra (Brockmann Consult GmbH), " \
             "Marco Zühlke (Brockmann Consult GmbH), " \
             "Chris Bernat (Telespazio VEGA UK Ltd)"


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
                     protocol: str = None) -> Any:
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
        :param monitor: a progress monitor.
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
    def is_local(self) -> bool:
        """
        Whether this is a remote data source not requiring any internet connection when its ``query()`` method
        is called or the ``open_dataset()`` and ``make_local()`` methods on one of its data sources.
        """
        return self._is_local

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


# noinspection PyArgumentList
class DataAccessError(Exception):
    """
    Exceptions produced by Cate's data stores and data sources instances, used to report any problems handling data.
    """
    def __init__(self, source, cause, *args, **kwargs):
        self._source = source
        if isinstance(source, DataSource):
            source_name = 'DataSource'
        elif isinstance(source, DataStore):
            source_name = 'DataStore'
        else:
            source_name = ""

        if source_name:
            if isinstance(cause, Exception):
                super(DataAccessError, self).__init__("{} '{}' returned error: {}".format(source_name, source.id,
                                                                                          str(cause)), *args, **kwargs)
            elif isinstance(cause, str):
                super(DataAccessError, self).__init__("{} '{}' returned error: {}".format(source_name, source.id,
                                                                                          cause), *args, **kwargs)
            else:
                super(DataAccessError, self).__init__(*args, **kwargs)
        else:
            if isinstance(cause, Exception):
                super(DataAccessError, self).__init__(str(cause), *args, **kwargs)
            elif isinstance(cause, str):
                super(DataAccessError, self).__init__(cause, *args, **kwargs)
            else:
                super(DataAccessError, self).__init__(*args, **kwargs)

    @property
    def cause(self):
        return self._cause


class DataAccessWarning(UserWarning):
    """
    Warnings produced by Cate's data stores and data sources instances, used to report any problems handling data.
    """
    pass


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
        raise ValueError('No data_source given')

    if isinstance(data_source, str):
        data_store_list = list(DATA_STORE_REGISTRY.get_data_stores())
        data_sources = find_data_sources(data_store_list, ds_id=data_source)
        if len(data_sources) == 0:
            raise ValueError("No data_source found for the given query term", data_source)
        elif len(data_sources) > 1:
            raise ValueError("%s data_sources found for the given query term '%s'" % (len(data_sources), data_source))
        data_source = data_sources[0]
        if force_local:
            data_source = data_source.make_local(local_name=local_ds_id if local_ds_id else "",
                                                 time_range=time_range, region=region, var_names=var_names,
                                                 monitor=monitor)
    return data_source.open_dataset(time_range, region, var_names)


# noinspection PyUnresolvedReferences,PyProtectedMember
def open_xarray_dataset(paths, concat_dim='time', **kwargs) -> xr.Dataset:
    """
    Open multiple files as a single dataset. This uses dask. If each individual file
    of the dataset is small, one dask chunk will coincide with one temporal slice,
    e.g. the whole array in the file. Otherwise smaller dask chunks will be used
    to split the dataset.

    :param paths: Either a string glob in the form "path/to/my/files/\*.nc" or an explicit
        list of files to open.
    :param concat_dim: Dimension to concatenate files along. You only
        need to provide this argument if the dimension along which you want to
        concatenate is not a dimension in the original datasets, e.g., if you
        want to stack a collection of 2D arrays along a third dimension.
    :param kwargs: Keyword arguments directly passed to ``xarray.open_mfdataset()``
    """
    # By default the dask chunk size of xr.open_mfdataset is (lat,lon,1). E.g.,
    # the whole array is one dask slice irrespective of chunking on disk.
    #
    # netCDF files can also feature a significant level of compression rendering
    # the known file size on disk useless to determine if the default dask chunk
    # will be small enough that a few of them could comfortably fit in memory for
    # parallel processing.
    #
    # Hence we open the first file of the dataset, find out its uncompressed size
    # and use that, together with an empirically determined threshold, to find out
    # the smallest amount of chunks such that each chunk is smaller than the
    # threshold and the number of chunks is a squared number so that both axes,
    # lat and lon could be divided evenly. We use a squared number to avoid
    # in addition to all of this finding the best way how to split the number of
    # chunks into two coefficients that would produce sane chunk shapes.
    #
    # When the number of chunks has been found, we use its root as the divisor
    # to construct the dask chunks dictionary to use when actually opening
    # the dataset.
    #
    # If the number of chunks is one, we use the default chunking.
    #
    # Check if the uncompressed file (the default dask Chunk) is too large to
    # comfortably fit in memory
    threshold = 250 * (2 ** 20)  # 250 MB

    # Find number of chunks as the closest larger squared number (1,4,9,..)
    try:
        temp_ds = xr.open_dataset(paths[0])
    except (OSError, RuntimeError):
        # netcdf4 >=1.2.2 raises RuntimeError
        # We have a glob not a list
        temp_ds = xr.open_dataset(glob.glob(paths)[0])

    n_chunks = ceil(sqrt(temp_ds.nbytes / threshold)) ** 2

    if n_chunks == 1:
        temp_ds.close()
        # The file size is fine
        # autoclose ensures that we can open datasets consisting of a number of
        # files that exceeds OS open file limit.
        return xr.open_mfdataset(paths,
                                 concat_dim=concat_dim,
                                 autoclose=True,
                                 **kwargs)

    # lat/lon names are not yet known
    lat = get_lat_dim_name(temp_ds)
    lon = get_lon_dim_name(temp_ds)
    n_lat = len(temp_ds[lat])
    n_lon = len(temp_ds[lon])

    # temp_ds is no longer used
    temp_ds.close()

    divisor = sqrt(n_chunks)

    # Chunking will pretty much 'always' be 2x2, very rarely 3x3 or 4x4. 5x5
    # would imply an uncompressed single file of ~6GB! All expected grids
    # should be divisible by 2,3 and 4.
    if not (n_lat % divisor == 0) or not (n_lon % divisor == 0):
        raise ValueError("Can't find a good chunking strategy for the given"
                         "data source. Are lat/lon coordinates divisible by "
                         "{}?".format(divisor))

    chunks = {lat: n_lat // divisor, lon: n_lon // divisor}

    return xr.open_mfdataset(paths,
                             concat_dim=concat_dim,
                             chunks=chunks,
                             autoclose=True,
                             **kwargs)


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
