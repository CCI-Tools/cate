"""
Module Description
==================

This module provides ECT's data access API.

Module Requirements
===================

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
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from datetime import datetime, timedelta
from io import StringIO, IOBase
from typing import Sequence, Union, List

from ect.core import Dataset
from ect.core.cdm_xarray import XArrayDatasetAdapter
from ect.core.io_xarray import open_xarray_dataset


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
    def catalogue(self) -> 'Catalogue':
        """The catalogue to which this data source belongs."""

    @abstractmethod
    def open_dataset(self, **constraints) -> Dataset:
        """
        Open a dataset with the given *constraints*.

        :param constraints: open constraints.
        """

    def matches_filter(self, **constraints) -> bool:
        """Test if this data source matches the given *constraints*."""
        if constraints:
            for key, value in constraints.items():
                if key == 'name' and value not in self.name:
                    return False
        return True


class Catalogue(metaclass=ABCMeta):
    """Represents a catalogue of data sources."""

    # TODO (mz, nf) - define constraints --> have a look at Iris Constraint class
    @abstractmethod
    def query(self, **constraints) -> Sequence[DataSource]:
        """
        Query this catalogue using the given *constraints*.

        :param constraints: Query constraints.
        :return: Sequence of data sources.
        """


class CatalogueRegistry:
    """
    Registry of :py:class:`Catalogue` objects.
    """

    def __init__(self):
        self._catalogues = dict()

    def get_catalogue(self, name: str) -> Catalogue:
        return self._catalogues.get(name, None)

    def get_catalogues(self) -> Sequence[Catalogue]:
        return self._catalogues.values()

    def add_catalogue(self, name: str, catalogue: Catalogue):
        self._catalogues[name] = catalogue

    def remove_catalogue(self, name: str):
        del self._catalogues[name]

    def __len__(self):
        return len(self._catalogues)


#: The catalogue registry of type :py:class:`CatalogueRegistry`.
CATALOGUE_REGISTRY = CatalogueRegistry()


def query_data_sources(catalogues: Union[Catalogue, Sequence[Catalogue]]=None, **constraints) \
        -> Sequence[DataSource]:
    """Query the catalogue(s) for data sources matching the given constrains.

    Parameters
    ----------
    catalogues : Catalogue or Sequence[Catalogue]
       If given these catalogues will be queried. Otherwise all registered catalogues will be used.
    constraints : dict, optional
       The contains may limit the dataset in space or time.

    Returns
    -------
    datasource : List[DataSource]
       All data sources matching the given constrains.

    See Also
    --------
    open_dataset
    """
    if catalogues is None:
        catalogue_list = CATALOGUE_REGISTRY.get_catalogues()
    elif isinstance(catalogues, Catalogue):
        catalogue_list = [catalogues]
    else:
        catalogue_list = catalogues
    results = []
    # noinspection PyTypeChecker
    for catalogue in catalogue_list:
        results.extend(catalogue.query(**constraints))
    return results


def open_dataset(data_source: Union[DataSource, str], **constraints) -> Dataset:
    """Load and decode a dataset.

    Parameters
    ----------
    data_source : str or DataSource
       Strings are interpreted as the identifier of an ECV dataset.
    constraints : str, optional
       The contains may limit the dataset in space or time.

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
        catalogue_list = CATALOGUE_REGISTRY.get_catalogues()
        data_sources = query_data_sources(catalogue_list, name=data_source)
        if len(data_sources) == 0:
            raise ValueError('No data_source found')
        elif len(data_sources) > 1:
            raise ValueError('%s data_sources found for the given query term' % len(data_sources))
        data_source = data_sources[0]
    return data_source.open_dataset(**constraints)


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
                 file_set_catalogue: 'FileSetCatalogue',
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
    def catalogue(self) -> 'FileSetCatalogue':
        return self._file_set_catalogue

    def open_dataset(self, **constraints) -> Dataset:
        first_time = constraints.get('first_time', None)
        last_time = constraints.get('last_time', None)
        paths = self.resolve_paths(first_time=first_time, last_time=last_time)
        # TODO (mz) - differentiate between xarray and shapefile
        xr_dataset = open_xarray_dataset(paths)
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
        return self._base_dir + "/" + self._file_pattern

    def resolve_paths(self, first_time: Union[str, datetime]=None, last_time: Union[str, datetime]=None) -> \
            Sequence[str]:
        """Return a list of all paths between the given times.

        For all dates, including the first and the last time, the wildcard in the pattern is resolved for the date.

        Parameters
        ----------
        first_time : str or datetime
            The first date of the time range, can be None if the file set has a *start_time*.
            In this case the *start_time* is used.
        last_time : str or datetime
            The last date of the time range, can be None if the file set has a *end_time*.
            In this case the *end_time* is used.
        """
        if first_time is None and (self._fileset_info is None or self._fileset_info.start_time is None):
            raise ValueError("neither first_time nor start_time are given")
        dt1 = _as_datetime(first_time, self._fileset_info.start_time)

        if last_time is None and (self._fileset_info is None or self._fileset_info.end_time is None):
            raise ValueError("neither last_time nor end_time are given")
        dt2 = _as_datetime(last_time, self._fileset_info.end_time)

        if dt1 > dt2:
            raise ValueError("start time '%s' is after end time '%s'" % (dt1, dt2))

        paths = [self._resolve_date(dt1 + timedelta(days=x)) for x in range((dt2 - dt1).days + 1)]
        if self.catalogue:
            paths = [self.catalogue.root_dir + '/' + p for p in paths]
        return paths

    # noinspection PyUnresolvedReferences
    def _resolve_date(self, dt: datetime):
        path = self._full_pattern
        if '{YYYY}' in path:
            path = path.replace('{YYYY}', '%04d' % dt.year)
        if '{MM}' in path:
            path = path.replace('{MM}', '%02d' % dt.month)
        if '{DD}' in path:
            path = path.replace('{DD}', '%02d' % dt.day)
        return path


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
        return dict(
            info_update_time=self._info_update_time,
            start_time=self._start_time,
            end_time=self._end_time,
            num_files=self._num_files,
            size_in_mb=self._size_in_mb,
        )

    @property
    def start_time(self):
        return self._start_time

    @property
    def end_time(self):
        return self._end_time


class FileSetCatalogue(Catalogue):
    def __init__(self, root_dir: str):
        self._root_dir = root_dir
        self._data_sources = []

    @property
    def root_dir(self) -> str:
        return self._root_dir

    def query(self, **constraints) -> Sequence[DataSource]:
        return [ds for ds in self._data_sources if ds.matches_filter(**constraints)]

    def expand_from_json(self, json_fp_or_str: Union[str, IOBase]):
        if isinstance(json_fp_or_str, str):
            fp = StringIO(json_fp_or_str)
        else:
            fp = json_fp_or_str
        for data in json.load(fp):
            if 'start_date' in data and 'end_date' in data and 'num_files' in data and 'size_mb' in data:
                file_set_info = FileSetInfo(
                    datetime.now(),  # TODO (mz) - put scan time into JSON
                    data['start_date'],
                    data['end_date'],
                    data['num_files'],
                    data['size_mb']
                )
            else:
                file_set_info = None
            self._data_sources.append(FileSetDataSource(
                self,
                data['name'],
                data['base_dir'],
                data['file_pattern'],
                fileset_info=file_set_info
            ))

    @classmethod
    def from_json(cls, root_dir: str, json_fp_or_str: Union[str, IOBase]) -> 'FileSetCatalogue':
        catalogue = FileSetCatalogue(root_dir)
        catalogue.expand_from_json(json_fp_or_str)
        return catalogue


def _as_datetime(dt: Union[str, datetime], default) -> datetime:
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
