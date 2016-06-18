"""
Module Description
==================

This module provides ECT's data access API.

Technical Requirements
======================

**Query catalogue**

:Description: Allow querying registered ECV catalogues using a simple function that takes a set of query parameters
    and returns data source identifiers that can be used to open respective ECV dataset in the ECT.
:Specified in: <link to other RST page here>
:Test: ``test_io.py``
:URD-Source:
    * CCIT-UR-DM0006: Data access to ESA CCI
    * CCIT-UR-DM0010: The data module shall have the means to attain meta-level status information per ECV type
    * CCIT-UR-DM0013: The CCI Toolbox shall allow filtering

----

**Add catalogue**

:Description: Allow adding of user defined catalogues specifying the access protocol and the layout of the data.
    These catalogues can be used to access datasets.
:Specified in: <link to other RST page here>
:Test: ``test_io.py``
:URD-Source:
    * CCIT-UR-DM0011: Data access to non-CCI data

----

**Open dataset**

:Description: Allow opening an ECV dataset given an identifier returned by the *catalogue query*.
   The dataset returned complies to the ECT common data model.
   The dataset to be returned can optionally be constrained in time and space.
:Specified in: <link to other RST page here>
:Test: ``test_io.py``
:URD-Source:
    * CCIT-UR-DM0001: Data access and input
    * CCIT-UR-DM0004: Open multiple inputs
    * CCIT-UR-DM0005: Data access using different protocols>
    * CCIT-UR-DM0007: Open single ECV
    * CCIT-UR-DM0008: Open multiple ECV
    * CCIT-UR-DM0009: Open any ECV
    * CCIT-UR-DM0012: Open different formats




Module Reference
================
"""
import json
import os
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from datetime import datetime, timedelta
from io import StringIO, IOBase
from typing import Sequence, Union, List, Tuple

from ect.core import Dataset
from ect.core.cdm_xarray import XArrayDatasetAdapter
from ect.core.io_xarray import open_xarray_dataset
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
        """Human-readable dataset name."""

    @property
    @abstractmethod
    def catalog(self) -> 'Catalog':
        """The catalogue to which this data source belongs."""

    @abstractmethod
    def open_dataset(self, time_range=None) -> Dataset:
        """
        Open a dataset with the given constraints.

        :param time_range: a tuple of datetime or str, optional. To limits the dataset in time.
        """

    def matches_filter(self, name=None) -> bool:
        """Test if this data source matches the given *constraints*."""
        if name and name != self.name:
            return False
        return True

    # noinspection PyMethodMayBeStatic
    def sync(self, monitor: Monitor = Monitor.NULL):
        """
        Synchronize remote data with locally stored data.
        The default implementation does nothing.

        :param monitor: a progress monitor.
        """
        pass

    @property
    def info_string(self):
        """Return some textual information about this data source. Useful for CLI / REPL applications."""
        return self.name

    def __str__(self):
        return self.info_string

    @abstractmethod
    def _repr_html_(self):
        """Provide an HTML representation of this object for IPython."""


class Catalog(metaclass=ABCMeta):
    """Represents a catalogue of data sources."""

    # TODO (mzuehlke, forman, 20160603): define constraints --> have a look at Iris Constraint class
    @abstractmethod
    def query(self, name=None) -> Sequence[DataSource]:
        """
        Query this catalogue using the given *constraints*.

        :param name: An optional name of the dataset.
        :return: Sequence of data sources.
        """

    @abstractmethod
    def _repr_html_(self):
        """Provide an HTML representation of this object for IPython."""


class CatalogRegistry:
    """
    Registry of :py:class:`Catalog` objects.
    """

    def __init__(self):
        self._catalogs = dict()

    def get_catalog(self, name: str) -> Catalog:
        return self._catalogs.get(name, None)

    def get_catalogs(self) -> Sequence[Catalog]:
        return self._catalogs.values()

    def add_catalog(self, name: str, catalogue: Catalog):
        self._catalogs[name] = catalogue

    def remove_catalog(self, name: str):
        del self._catalogs[name]

    def __len__(self):
        return len(self._catalogs)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        import pprint
        return pprint.pformat(self._catalogs)

    def _repr_html_(self):
        rows = []
        for name, cat in self._catalogs.items():
            rows.append('<tr><td>%s</td><td>%s</td></tr>' % (name, repr(cat)))
        return '<table>%s</table>' % '\n'.join(rows)


#: The data catalogue registry of type :py:class:`CatalogueRegistry`.
#: Use it add new data catalogues to ECT.
CATALOG_REGISTRY = CatalogRegistry()


def query_data_sources(catalogs: Union[Catalog, Sequence[Catalog]] = None, name=None) -> Sequence[DataSource]:
    """Query the catalogue(s) for data sources matching the given constrains.

    Parameters
    ----------
    catalogs : Catalog or Sequence[Catalog]
       If given these catalogues will be queried. Otherwise all registered catalogues will be used.
    name : str, optional
       The name of the dataset.

    Returns
    -------
    datasource : List[DataSource]
       All data sources matching the given constrains.

    See Also
    --------
    open_dataset
    """
    if catalogs is None:
        catalogue_list = CATALOG_REGISTRY.get_catalogs()
    elif isinstance(catalogs, Catalog):
        catalogue_list = [catalogs]
    else:
        catalogue_list = catalogs
    results = []
    # noinspection PyTypeChecker
    for catalogue in catalogue_list:
        results.extend(catalogue.query(name))
    return results


def open_dataset(data_source: Union[DataSource, str], time_range=None) -> Dataset:
    """Load and decode a dataset.

    Parameters
    ----------
    data_source : str or DataSource
       Strings are interpreted as the identifier of an ECV dataset.
    time_range : a tuple of datetime or str, optional
       The *time_range*, if given, limits the dataset in time.

    Returns
    -------
    dataset : Dataset
       The newly created dataset.

    See Also
    --------
    query_data_sources
    """
    if data_source is None:
        raise ValueError('No data_source given')

    if isinstance(data_source, str):
        catalogue_list = CATALOG_REGISTRY.get_catalogs()
        data_sources = query_data_sources(catalogue_list, name=data_source)
        if len(data_sources) == 0:
            raise ValueError('No data_source found')
        elif len(data_sources) > 1:
            raise ValueError('%s data_sources found for the given query term' % len(data_sources))
        data_source = data_sources[0]
    return data_source.open_dataset(time_range)


class FileSetDataSource(DataSource):
    """A class representing the a specific file set with the meta information belonging to it.

    Parameters
    ----------
    name : str
        The name of the file set
    base_dir : str
        The base directory
    file_pattern : str
        The file pattern with wildcards for year, month, and day
    fileset_info : FileSetInfo
        The file set info generated by a scanning, can be None

    Returns
    -------
    new  : FileSetDataSource
    """

    def __init__(self,
                 file_set_catalogue: 'FileSetCatalog',
                 name: str,
                 base_dir: str,
                 file_pattern: str,
                 fileset_info: 'FileSetInfo' = None):
        self._file_set_catalogue = file_set_catalogue
        self._name = name
        self._base_dir = base_dir
        self._file_pattern = file_pattern
        self._fileset_info = fileset_info

    @property
    def name(self):
        return self._name

    @property
    def catalog(self) -> 'FileSetCatalog':
        return self._file_set_catalogue

    def open_dataset(self, time_range=None) -> Dataset:
        paths = self.resolve_paths(time_range)
        unique_paths = list(set(paths))
        existing_paths = [p for p in unique_paths if os.path.exists(p)]
        # TODO (mzuehlke, 20160603): differentiate between xarray and shapefile
        xr_dataset = open_xarray_dataset(existing_paths)
        cdm_dataset = XArrayDatasetAdapter(xr_dataset)
        return cdm_dataset

    def to_json_dict(self):
        """
        Return a JSON-serializable dictionary representation of this object.

        :return: A JSON-serializable dictionary
        """
        fsds_dict = OrderedDict()
        fsds_dict['name'] = self.name
        fsds_dict['base_dir'] = self._base_dir
        fsds_dict['file_pattern'] = self._file_pattern
        if self._fileset_info:
            fsds_dict['fileset_info'] = self._fileset_info.to_json_dict()
        return fsds_dict

    @property
    def _full_pattern(self) -> str:
        return self._base_dir + '/' + self._file_pattern

    def resolve_paths(self, time_range: TimeRange = (None, None)) -> List[str]:
        """Return a list of all paths between the given times.

        For all dates, including the first and the last time, the wildcard in the pattern is resolved for the date.

        Parameters
        ----------
        time_range : a tuple of datetime or str, optional
               The *time_range*, if given, limits the dataset in time.
               The first date of the time range, can be None if the file set has a *start_time*.
               In this case the *start_time* is used.
               The last date of the time range, can be None if the file set has a *end_time*.
               In this case the *end_time* is used.
        """
        return [self.catalog.root_dir + '/' + p for p in self.resolve_base_paths(time_range)]

    def resolve_base_paths(self, time_range: TimeRange = (None, None)) -> List[str]:
        """Return a list of all paths between the given times.

        For all dates, including the first and the last time, the wildcard in the pattern is resolved for the date.

        Parameters
        ----------
        time_range : a tuple of datetime or str, optional
               The *time_range*, if given, limits the dataset in time.
               The first date of the time range, can be None if the file set has a *start_time*.
               In this case the *start_time* is used.
               The last date of the time range, can be None if the file set has a *end_time*.
               In this case the *end_time* is used.
        """

        date1 = _as_datetime(time_range[0], self._fileset_info.start_time if self._fileset_info else None)
        date2 = _as_datetime(time_range[1], self._fileset_info.end_time if self._fileset_info else None)

        if date1 is None:
            raise ValueError("illegal time_range: can't determine start of interval")

        if date2 is None:
            raise ValueError("illegal time_range: can't determine end of interval")

        if date1 > date2:
            raise ValueError("start time '%s' is after end time '%s'" % (date1, date2))

        return [self._resolve_base_path(date1 + timedelta(days=i)) for i in range((date2 - date1).days + 1)]

    def _resolve_base_path(self, date: datetime):
        resolved_path = self._file_pattern
        resolved_path = resolved_path.replace('{YYYY}', '%04d' % date.year)
        resolved_path = resolved_path.replace('{MM}', '%02d' % date.month)
        resolved_path = resolved_path.replace('{DD}', '%02d' % date.day)
        return self._base_dir + '/' + resolved_path

    def sync(self, monitor: Monitor = Monitor.NULL):
        """
        Synchronize remote data with locally stored data.
        The default implementation does nothing.

        :param monitor: a progress monitor.
        """
        paths = self.resolve_paths()
        with monitor.starting("Synchronising", total_work=len(paths)):
            for path in paths:
                if monitor.is_cancelled():
                    return
                self._sync_file(path, monitor.child(1))

    def _sync_file(self, path, monitor):
        with monitor.starting(path, 1):
            # TODO (forman, 20160617) - rsync FTP and local dir here
            monitor.progress(1)

    def __repr__(self):
        return "FileSetDataSource(%s, %s, %s)" % (repr(self._name), repr(self._base_dir), repr(self._file_pattern))

    @property
    def info_string(self):
        table_data = self.get_table_data()
        if self._fileset_info:
            table_data.update(self._fileset_info.get_table_data())
        return '\n'.join(['%s: %s' % (name, value)
                          for name, value in table_data.items()])

    def _repr_html_(self):
        import html
        table_data = self.get_table_data()
        if self._fileset_info:
            table_data.update(self._fileset_info.get_table_data())
        rows = '\n'.join(['<tr><td>%s</td><td><strong>%s</strong></td></tr>' % (name, html.escape(str(value)))
                          for name, value in table_data.items()])
        return '<table style="border:0;">%s</table>' % rows

    def get_table_data(self):
        return OrderedDict([('Name', self._name),
                            ('Base directory', self._base_dir),
                            ('File pattern', self._file_pattern)])


class FileSetInfo:
    def __init__(self,
                 info_update_time: Union[str, datetime],
                 start_time: Union[str, datetime],
                 end_time: Union[str, datetime],
                 num_files: int,
                 size_in_mb: int):
        self._info_update_time = _as_datetime(info_update_time, None)
        self._start_time = _as_datetime(start_time, None)
        self._end_time = _as_datetime(end_time, None)
        self._num_files = num_files
        self._size_in_mb = size_in_mb

    def to_json_dict(self):
        """
        Return a JSON-serializable dictionary representation of this object.

        :return: A JSON-serializable dictionary
        """
        return dict(info_update_time=self._info_update_time,
                    start_time=self._start_time,
                    end_time=self._end_time,
                    num_files=self._num_files,
                    size_in_mb=self._size_in_mb)

    @property
    def info_update_time(self):
        return self._info_update_time

    @property
    def start_time(self):
        return self._start_time

    @property
    def end_time(self):
        return self._end_time

    @property
    def num_files(self):
        return self._num_files

    @property
    def size_in_mb(self):
        return self._size_in_mb

    def info_string(self):
        table_data = self.get_table_data()
        return '\n'.join(['%s:\t\t%s' % (name, str(value)) for name, value in table_data.items()])

    def _repr_html_(self):
        import html
        table_data = self.get_table_data()
        return '<table style="border:0;">%s</table>' % '\n'.join(
            ['<tr><td>%s</td><td><strong>%s</strong></td></tr>' % (name, html.escape(str(value)))
             for name, value in table_data.items()])

    def get_table_data(self):
        table_data = OrderedDict([('Last update time', self._info_update_time),
                                  ('Data start time', self._start_time),
                                  ('Data stop time', self._end_time),
                                  ('#Files', self._num_files),
                                  ('Size (MB)', self._size_in_mb),
                                  ])
        return table_data


class FileSetCatalog(Catalog):
    """
    A catalogue for a fileset in the the operating system's file system.
    It is composed of data sources of type :py:class:`FileSetDataSource` that are represented by
    the operating system's file system.

    :param root_dir: The path to the fileset's root directory.
    :param remote_url: Optional URL of the catalogue's remote service.
    """

    def __init__(self, root_dir: str, remote_url:str = None):
        self._root_dir = root_dir
        self._remote_url = remote_url
        self._data_sources = []

    @property
    def root_dir(self) -> str:
        """The path to the fileset's root directory."""
        return self._root_dir

    @property
    def remote_url(self) -> str:
        """Optional URL of the catalogue's remote service."""
        return self._remote_url

    def query(self, name=None) -> Sequence[DataSource]:
        return [ds for ds in self._data_sources if ds.matches_filter(name)]

    def load_from_json(self, json_fp_or_str: Union[str, IOBase]):
        if isinstance(json_fp_or_str, str):
            fp = StringIO(json_fp_or_str)
        else:
            fp = json_fp_or_str
        catalog_dict = json.load(fp)
        remote_url = catalog_dict.get('remote_url', self._remote_url)
        data_sources_json = catalog_dict.get('data_sources', [])
        data_sources = []
        for data in data_sources_json:
            file_set_info = None
            if 'start_date' in data and 'end_date' in data and 'num_files' in data and 'size_mb' in data:
                # TODO (mzuehlke, 20160603): used named parameters
                file_set_info = FileSetInfo(datetime.now(),  # TODO (mzuehlke, 20160603): include scan time in JSON
                                            data['start_date'],
                                            data['end_date'],
                                            data['num_files'],
                                            data['size_mb'])

            # TODO (mzuehlke, 20160603): used named parameters
            file_set_data_source = FileSetDataSource(self,
                                                     # TODO (mzuehlke, 20160603): change this in the JSON file
                                                     data['name'].replace('/', '_').upper(),
                                                     data['base_dir'],
                                                     data['file_pattern'],
                                                     fileset_info=file_set_info)
            data_sources.append(file_set_data_source)

        self._remote_url = remote_url
        self._data_sources.extend(data_sources)

    @classmethod
    def from_json(cls, root_dir: str, json_fp_or_str: Union[str, IOBase]) -> 'FileSetCatalog':
        catalogue = FileSetCatalog(root_dir)
        catalogue.load_from_json(json_fp_or_str)
        return catalogue

    def __repr__(self):
        return "FileSetCatalogue(%s)" % repr(self._root_dir)

    def _repr_html_(self):
        rows = []
        row_count = 0
        for ds in self._data_sources:
            row_count += 1
            rows.append('<tr><td><strong>%s</strong></td><td>%s</td></tr>' % (row_count, ds._repr_html_()))
        return '<p>Contents of FileSetCatalogue for root <code>%s<code></p><table>%s</table>' % (
            self._root_dir, '\n'.join(rows))


def _as_datetime(dt: Time, default) -> datetime:
    if dt is None:
        return default
    if isinstance(dt, str):
        if dt == '':
            return default
        try:
            return datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return datetime.strptime(dt, "%Y-%m-%d")
    if isinstance(dt, datetime):
        return dt
    raise TypeError()
