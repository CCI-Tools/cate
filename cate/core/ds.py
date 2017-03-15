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

__author__ = "Marco Zühlke (Brockmann Consult GmbH)"

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
`test/test_ds.py <https://github.com/CCI-Tools/cate-core/blob/master/test/test_ds.py>`_
and may be executed using ``$ py.test test/test_ds.py --cov=cate/core/ds.py`` for extra code coverage information.


Components
==========
"""
import os.path
import glob
from math import ceil, sqrt
from abc import ABCMeta, abstractmethod
from datetime import datetime, date
from typing import Sequence, Optional
from typing import Union, Tuple

import xarray as xr

from cate.conf.defaults import DEFAULT_DATA_PATH
from cate.conf import get_config_path
from cate.core.cdm import Schema, get_lon_dim_name, get_lat_dim_name
from cate.util.misc import to_datetime_range
from cate.util.monitor import Monitor


def get_data_stores_path() -> str:
    """
    Get the default path to where Cate stores local data store information and stores data files synchronized with their
    remote versions.

    :return: Effectively reads the value of the configuration parameter ``data_stores_path``, if any. Otherwise return
             the default value ``~/.cate/data_stores``.
    """
    return get_config_path('data_stores_path', os.path.join(DEFAULT_DATA_PATH, 'data_stores'))


class DataSource(metaclass=ABCMeta):
    """
    An abstract data source from which datasets can be retrieved.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable data source name."""

    @property
    def schema(self) -> Optional[Schema]:
        """The data :py:class:`Schema` for any dataset provided by this data source or ``None`` if unknown."""
        return None

    def temporal_coverage(self, monitor: Monitor=Monitor.NONE) -> Optional[Tuple[datetime, datetime]]:
        """
        The temporal coverage as tuple (*start*, *end*) where *start* and *end* are UTC ``datetime`` instances.

        :param monitor: a progress monitor.
        :return A tuple of (*start*, *end*) UTC ``datetime`` instances or ``None`` if the temporal coverage is unknown.
        """
        return None

    @property
    @abstractmethod
    def data_store(self) -> 'DataStore':
        """The data store to which this data source belongs."""

    def matches_filter(self, name=None) -> bool:
        """Test if this data source matches the given *constraints*."""
        if name and name != self.name:
            return False
        return True

    @abstractmethod
    def open_dataset(self,
                     time_range: Tuple[datetime, datetime]=None,
                     protocol: str=None) -> xr.Dataset:
        """
        Open a dataset for the given *time_range*.


        :param time_range: An optional tuple comprising a start and end date,
                which must be ``datetime.datetime`` objects.
        :param protocol: Protocol name, if None selected default protocol
                will be used to access data
        :return: A dataset instance or ``None`` if no data is available in *time_range*.
        """

    @property
    def protocols(self) -> []:
        """
        The list of available protocols.

        """
        return [None]

    # noinspection PyMethodMayBeStatic
    def sync(self,
             time_range: Tuple[datetime, datetime]=None,
             protocol: str=None,
             monitor: Monitor=Monitor.NONE) -> Tuple[int, int]:
        """
        Allows to synchronize remote data with locally stored data.
        Availability of synchronization feature depends on protocol type and
        data source implementation.
        The default implementation does nothing.

        :param time_range: An optional tuple comprising a start and end date,
                which must be ``datetime.datetime`` objects.
        :param protocol: Protocol name, if None selected default protocol
                will be used to access data
        :param monitor: a progress monitor.
        :return: a tuple: (synchronized number of selected files, total number of selected files)
        """
        return 0, 0

    def delete_local(self,
                     time_range: Tuple[datetime, datetime]) -> int:
        """
        Delete locally stored data.
        The default implementation does nothing.

        :param time_range: An optional tuple comprising a start and end date, which must be
               ``datetime.datetime`` objects.
        :return: removed number of files
        """
        return 0

    @property
    def meta_info(self) -> Union[dict, None]:
        """
        Return meta-information about this data source.
        The returned dict, if any, is JSON-serializable.
        """
        return None

    @property
    def cache_info(self) -> Union[dict, None]:
        """
        Return information about cached, locally available data sets.
        The returned dict, if any, is JSON-serializable.
        """
        return None

    @property
    def variables_info(self) -> Union[dict, None]:
        """
        Return meta-information about the variables contained in this data source.
        The returned dict, if any, is JSON-serializable.
        """
        return None

    @property
    def info_string(self):
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

    @property
    def variables_info_string(self):
        """
        Return some textual information about the variables contained in this data source.
        Useful for CLI / REPL applications.
        """
        meta_info = self.meta_info
        if not meta_info or meta_info.get('variables', None) is None:
            return 'No variables information available.'

        variables = meta_info['variables']
        info_lines = []
        for variable in variables:
            info_lines.append('%s (%s):' % (variable.get('name', '?'), variable.get('units', '-')))
            info_lines.append('  Long name:        %s' % variable.get('long_name', '?'))
            info_lines.append('  CF standard name: %s' % variable.get('standard_name', '?'))
            info_lines.append('')

        return '\n'.join(info_lines)

    @property
    def cached_datasets_coverage_string(self):
        """
        Return a textual representation of information about cached, locally available data sets.
        Useful for CLI / REPL applications.
        """
        cache_coverage = self.cache_info
        if not cache_coverage:
            return 'No information about cached datasets available.'

        info_lines = []
        for date_from, date_to in sorted(cache_coverage.items()):
            info_lines.append('{date_from} to {date_to}'
                              .format(
                                date_from=date_from.strftime('%Y-%m-%d'),
                                date_to=date_to.strftime('%Y-%m-%d')))

        return '\n'.join(info_lines)

    def __str__(self):
        return self.info_string

    @abstractmethod
    def _repr_html_(self):
        """Provide an HTML representation of this object for IPython."""


class DataStore(metaclass=ABCMeta):
    """Represents a data store of data sources."""

    # Check Iris "Constraint" class to implement user-friendly, efficient filters (mzuehlke, forman, 20160603)

    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        """
        Return he name of this data store.
        """
        return self._name

    @abstractmethod
    def query(self, name=None, monitor: Monitor = Monitor.NONE) -> Sequence[DataSource]:
        """
        Retrieve data sources in this data store using the given constraints.

        :param name: Name of the data source.
        :param monitor:  A progress monitor.
        :return: Sequence of data sources.
        """

    def update_indices(self, update_file_lists: bool = False, monitor: Monitor = Monitor.NONE):
        """
        Update this data store's indices to speed up queries and to fetch meta-information about its
        contained data sources.

        The default implementation is a no-op.

        :param update_file_lists: To also update the a data source's contained file lists (if any)
        :param monitor:  A progress monitor.
        """

    @abstractmethod
    def _repr_html_(self):
        """Provide an HTML representation of this object for IPython."""


class DataStoreRegistry:
    """
    Registry of :py:class:`DataStore` objects.
    """

    def __init__(self):
        self._data_stores = dict()

    def get_data_store(self, name: str) -> Optional[DataStore]:
        return self._data_stores.get(name, None)

    def get_data_stores(self) -> Sequence[DataStore]:
        return self._data_stores.values()

    def add_data_store(self, data_store: DataStore):
        self._data_stores[data_store.name] = data_store

    def remove_data_store(self, name: str):
        del self._data_stores[name]

    def __len__(self):
        return len(self._data_stores)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        import pprint
        return pprint.pformat(self._data_stores)

    def _repr_html_(self):
        rows = []
        for name, data_store in self._data_stores.items():
            rows.append('<tr><td>%s</td><td>%s</td></tr>' % (name, repr(data_store)))
        return '<table>%s</table>' % '\n'.join(rows)


#: The data data store registry of type :py:class:`DataStoreRegistry`.
#: Use it add new data stores to Cate.
DATA_STORE_REGISTRY = DataStoreRegistry()


def query_data_sources(data_stores: Union[DataStore, Sequence[DataStore]] = None, name=None) -> Sequence[DataSource]:
    """Query the data store(s) for data sources matching the given constrains.

    See also :py:func:`open_dataset`.

    :param data_stores: If given these data stores will be queried. Otherwise all registered data stores will be used.
    :param name:  The name of a data source.
    :return: All data sources matching the given constrains.
    """
    if data_stores is None:
        data_store_list = DATA_STORE_REGISTRY.get_data_stores()
    elif isinstance(data_stores, DataStore):
        data_store_list = [data_stores]
    else:
        data_store_list = data_stores
    results = []
    # noinspection PyTypeChecker
    for data_store in data_store_list:
        results.extend(data_store.query(name))
    return results


def open_dataset(data_source: Union[DataSource, str],
                 start_date: Union[None, str, date] = None,
                 end_date: Union[None, str, date] = None,
                 sync: bool = False,
                 protocol: str=None,
                 monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Open a dataset from a data source.

    :param data_source: Strings are interpreted as the identifier of an ECV dataset.
    :param start_date: Optional start date of the requested dataset
    :param end_date: Optional end date of the requested dataset
    :param sync: Whether to synchronize local and remote data files before opening the dataset
    :param protocol: Name of protocol used to access dataset
    :param monitor: a progress monitor, used only if *sync* is ``True``
    :return: An new dataset instance
    """
    if data_source is None:
        raise ValueError('No data_source given')

    if isinstance(data_source, str):
        data_store_list = DATA_STORE_REGISTRY.get_data_stores()
        data_sources = query_data_sources(data_store_list, name=data_source)
        if len(data_sources) == 0:
            raise ValueError("No data_source found for the given query term '%s'" % data_source)
        elif len(data_sources) > 1:
            raise ValueError("%s data_sources found for the given query term '%s'" % (len(data_sources), data_source))
        data_source = data_sources[0]

    time_range = to_datetime_range(start_date, end_date)

    if sync:
        data_source.sync(time_range, monitor=monitor, protocol=protocol)

    return data_source.open_dataset(time_range, protocol=protocol)


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
    # will be small enough that a few of them ccould comfortably fit in memory for 
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
        # The file size is fine
        return xr.open_mfdataset(paths, concat_dim=concat_dim, **kwargs)

    divisor = sqrt(n_chunks)

    # lat/lon names are not yet known
    lat = get_lat_dim_name(temp_ds)
    lon = get_lon_dim_name(temp_ds)
    n_lat = len(temp_ds[lat])
    n_lon = len(temp_ds[lon])

    # temp_ds is no longer used
    temp_ds.close()

    if n_chunks == 1:
        # The file size is fine
        return xr.open_mfdataset(paths, concat_dim=concat_dim, **kwargs)

    divisor = sqrt(n_chunks)

    # Chunking will pretty much 'always' be 2x2, very rarely 3x3 or 4x4. 5x5
    # would imply an uncompressed single file of ~6GB! All expected grids
    # should be divisible by 2,3 and 4.
    if not (n_lat % divisor == 0) or not (n_lon % divisor == 0):
        raise ValueError("Can't find a good chunking strategy for the given"
                         "data source. Are lat/lon coordinates divisible by "
                         "{}?".format(divisor))

    chunks = {lat: n_lat//divisor, lon: n_lon//divisor}

    return xr.open_mfdataset(paths, concat_dim=concat_dim, chunks=chunks, **kwargs)
