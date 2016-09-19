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

This module provides ECT's data access API.

Technical Requirements
======================

**Query data store**

:Description: Allow querying registered ECV data stores using a simple function that takes a set of query parameters
    and returns data source identifiers that can be used to open respective ECV dataset in the ECT.

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
   The dataset returned complies to the ECT common data model.
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

The module's unit-tests are located in `test/test_io.py <https://github.com/CCI-Tools/ect-core/blob/master/test/test_io.py>`_
and may be executed using ``$ py.test test/test_io.py --cov=ect/core/io.py`` for extra code coverage information.


Components
==========
"""
from abc import ABCMeta, abstractmethod
from datetime import datetime, date
from glob import glob
from typing import Sequence
from typing import Union, List, Tuple

import pandas as pd
import xarray as xr
from ect.core.cdm import Schema
from ect.core.monitor import Monitor

Time = Union[str, datetime]
TimeRange = Tuple[Time, Time]


class DataSource(metaclass=ABCMeta):
    """
    An abstract data source from which datasets can be retrieved.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable data source name."""

    @property
    def schema(self) -> Schema:
        """The data :py:class:`Schema` for any dataset provided by this data source or ``None`` if unknown."""
        return None

    @property
    def temporal_coverage(self):
        """
        The temporal coverage as tuple (*start*, *end*) where *start* and *and* are ``datetime`` instances.
        Return ``None`` if the temporal coverage is unknown.
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
                     start_date: Union[None, str, date] = None,
                     end_date: Union[None, str, date] = None) -> xr.Dataset:
        """
        Open a dataset with the given constraints. If *sync* is True, :py:meth:`sync` is called first.

        :param start_date: Optional start date of the requested dataset
        :param end_date: Optional end date of the requested dataset
        :return: A dataset or ``None`` if no data is available in *time_range*.
        """

    # noinspection PyMethodMayBeStatic
    def sync(self,
             start_date: Union[None, str, date] = None,
             end_date: Union[None, str, date] = None,
             monitor: Monitor = Monitor.NULL) -> Tuple[int, int]:
        """
        Synchronize remote data with locally stored data.
        The default implementation does nothing.

        :param start_date: Optional start date of the requested dataset
        :param end_date: Optional end date of the requested dataset
        :param monitor: a progress monitor.
        :return: a tuple (synchronized number of selected files, total number of selected files)
        """
        pass

    # TODO (forman, 20160916): (also) return JSON-dict so we can use the data source meta-data more flexible
    @property
    def info_string(self):
        """
        Return some textual information about this data source.
        Useful for CLI / REPL applications.
        """
        return 'No data source meta-data available.'

    # TODO (forman, 20160916): (also) return JSON-dict so we can use the variables meta-data more flexible
    @property
    def variables_info_string(self):
        """
        Return some textual information about the variables contained in this data source.
        Useful for CLI / REPL applications.
        """
        return 'Not variables meta-data available.'

    def __str__(self):
        return self.info_string

    @abstractmethod
    def _repr_html_(self):
        """Provide an HTML representation of this object for IPython."""


class DataStore(metaclass=ABCMeta):
    """Represents a data store of data sources."""

    # Check Iris "Constraint" class to implement user-friendly, efficient filters (mzuehlke, forman, 20160603)

    @abstractmethod
    def query(self, name=None, monitor: Monitor = Monitor.NULL) -> Sequence[DataSource]:
        """
        Retrieve data sources in this data store using the given constraints.

        :param name: Name of the data source.
        :param monitor:  A progress monitor.
        :return: Sequence of data sources.
        """

    def update_indices(self, update_file_lists: bool = False, monitor: Monitor = Monitor.NULL):
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

    def get_data_store(self, name: str) -> DataStore:
        return self._data_stores.get(name, None)

    def get_data_stores(self) -> Sequence[DataStore]:
        return self._data_stores.values()

    def add_data_store(self, name: str, data_store: DataStore):
        self._data_stores[name] = data_store

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
#: Use it add new data stores to ECT.
DATA_STORE_REGISTRY = DataStoreRegistry()


def query_data_sources(data_stores: Union[DataStore, Sequence[DataStore]] = None, name=None) -> Sequence[DataSource]:
    """Query the data store(s) for data sources matching the given constrains.

    Parameters
    ----------
    data_stores : DataStore or Sequence[DataStore]
       If given these data stores will be queried. Otherwise all registered data stores will be used.
    name : str, optional
       The name of a data source.

    Returns
    -------
    data_source : List[DataSource]
       All data sources matching the given constrains.

    See Also
    --------
    open_dataset
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
                 monitor: Monitor = Monitor.NULL) -> xr.Dataset:
    """
    Open a dataset from a data source.

    :param data_source: Strings are interpreted as the identifier of an ECV dataset.
    :param start_date: Optional start date of the requested dataset
    :param end_date: Optional end date of the requested dataset
    :param sync: Whether to synchronize local and remote data files before opening the dataset
    :param monitor: a progress monitor, used only if *snyc* is ``True``
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

    if sync:
        data_source.sync(start_date, end_date, monitor=monitor)

    return data_source.open_dataset(start_date, end_date)


# noinspection PyUnresolvedReferences,PyProtectedMember
def open_xarray_dataset(paths, preprocess=True, chunks=None, **kwargs) -> xr.Dataset:
    """
    Adapted version of the xarray 'open_mfdataset' function.
    """
    if isinstance(paths, str):
        paths = sorted(glob(paths))
    if not paths:
        raise IOError('no files to open')

    if not preprocess:
        return xr.open_mfdataset(paths, concat_dim='time')

    # TODO (forman, 20160916): marcoz, please cleanup the following code or at least comment what's going on here!

    # open all datasets
    lock = xr.backends.api._default_lock(paths[0], None)

    datasets = []
    engine = 'netcdf4'
    for p in paths:
        datasets.append(xr.open_dataset(p, engine=engine, decode_cf=False, chunks=chunks or {}, lock=lock, **kwargs))

    preprocessed_datasets = []
    file_objs = []
    for ds in datasets:
        pds = _preprocess_datasets(ds)
        if pds is None:
            ds._file_obj.close()
        else:
            pds_decoded = xr.decode_cf(pds)
            preprocessed_datasets.append(pds_decoded)
            file_objs.append(ds._file_obj)

    combined_datasets = _combine_datasets(preprocessed_datasets)
    combined_datasets._file_obj = xr.backends.api._MultiFileCloser(file_objs)
    return combined_datasets


def _combine_datasets(datasets: Sequence[xr.Dataset]) -> xr.Dataset:
    """
    Combines all datasets into a single.
    """
    if not datasets:
        raise ValueError('datasets argument must be a sequence of datasets')
    if 'time' in datasets[0].dims:
        return xr.auto_combine(datasets, concat_dim='time')
    else:
        time_index = [_extract_time_index(ds) for ds in datasets]
        return xr.concat(datasets, pd.Index(time_index, name='time'))


def _preprocess_datasets(dataset: xr.Dataset) -> xr.Dataset:
    """
    Modifies datasets, so that it is netcdf-CF compliant
    """
    for var in dataset.data_vars:
        attrs = dataset[var].attrs
        if '_FillValue' in attrs and 'missing_value' in attrs:
            # xarray as of version 0.7.2 does not handle it correctly,
            # if both values are set to NaN. (because the values are compared using '==')
            # reproducible with  engine='netcdf4'
            # https://github.com/pydata/xarray/issues/997
            del attrs['missing_value']
    return dataset


def _extract_time_index(ds: xr.Dataset) -> datetime:
    # TODO (forman, 20160916): marcoz, how can we be sure time_coverage_start/_end exist?
    time_coverage_start = ds.attrs['time_coverage_start']
    time_coverage_end = ds.attrs['time_coverage_end']
    try:
        # print(time_coverage_start, time_coverage_end)
        time_start = datetime.strptime(time_coverage_start, "%Y%m%dT%H%M%SZ")
        time_end = datetime.strptime(time_coverage_end, "%Y%m%dT%H%M%SZ")
        return time_end
    except ValueError:
        return None
