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
:Test: <link to test class.function here>
:URD-Source: <name the URD # and optionally the name>

----

**Open dataset**

:Description: Allow opening an ECV dataset given an identifier returned by the *catalogue query*.
   The dataset returned complies to the ECT common data model.
   The dataset to be returned can optionally be constrained in time and space.
:Specified in: <link to other RST page here>
:Test: <link to test class.function here>
:URD-Source: <name the URD # and optioanlly the name>



Module Reference
================
"""
from typing import Sequence, Union, List
from ect.core import Dataset
import json
from datetime import date, datetime, timedelta


class DataSource:
    def __init__(self, name: str, glob: str):
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def open_dataset(self, **constraints) -> Dataset:
        return None

    def matches_constrains(self, **constraints) -> bool:
        if constraints:
            for key, value in constraints.items():
                if key == 'name' and not value in self._name:
                    return False
        return True


class Catalogue:
    def __init__(self, *data_sources: DataSource):
        self._data_sources = data_sources

    def filter(self, **constraints) -> [DataSource]:
        return [ds for ds in self._data_sources if ds.matches_constrains(**constraints)]


DEFAULT_CATALOGUE = Catalogue(DataSource("default", "default"))


def query_data_sources(catalogues: Union[Catalogue, Sequence[Catalogue]] = DEFAULT_CATALOGUE, **constraints) -> List[
    DataSource]:
    """Queries the catalogue(s) for data sources matching the given constrains.

    Parameters
    ----------
    catalogues : Catalogue or Sequence[Catalogue]
       If given these catalogues will be querien. Othewise the DEFAULT_CATALOGUE will be used
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

    if isinstance(catalogues, Catalogue):
        catalogue_list = [catalogues]
    else:
        catalogue_list = catalogues
    results = []
    for catalogue in catalogue_list:
        results.extend(catalogue.filter(**constraints))
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
        data_source = query_data_sources(DEFAULT_CATALOGUE, name=data_source)
    return data_source.open_dataset(**constraints)


#########################


class FileSetType:
    """A class representing the a specific file set with the meta information belonging to it.

    Parameters
    ----------
    name : str
        The name of the file set
    base_dir : str
        The base directory
    start_date : str
        The start of the time range covered by this file set, can be None
    end_date : str
        The end of the time range covered by this file set, can be None
    num_files : int
        The number of files contained in this file set
    size_in_mb : int
        The number of files this file set contains
    file_pattern : str
        The file pattern with wildcards for year, month, and day

    Returns
    -------
    new  : FileSetType
    """
    def __init__(self, name: str, base_dir: str,
                 start_date: Union[str,date], end_date: Union[str,date],
                 num_files: int, size_in_mb: int, file_pattern: str):
        self._name = name
        self._base_dir = base_dir
        self._start_date = FileSetType._as_date(start_date, None)
        self._end_date = FileSetType._as_date(end_date, None)
        self._num_files = num_files
        self._size_in_mb = size_in_mb
        self._file_pattern = file_pattern

    @property
    def name(self) -> str:
        return self._name

    @property
    def base_dir(self) -> str:
        return self._base_dir

    @property
    def start_date(self) -> date:
        return self._start_date

    @property
    def end_date(self) -> date:
        return self._end_date

    @property
    def num_files(self) -> int:
        return self._num_files

    @property
    def size_in_mb(self) -> int:
        return self._size_in_mb

    @property
    def file_pattern(self) -> str:
        return self._file_pattern

    @property
    def full_pattern(self) -> str:
        return self.base_dir + "/" + self.file_pattern

    def resolve_paths(self, first_date: Union[str,date] = None, last_date: Union[str,date] = None) -> Sequence[str]:
        """Return a list of all paths between the given dates.

        For all dates, including the first and the last date, the wildcard in the pattern is resolved for the date.

        Parameters
        ----------
        first_date : str
            The first date of the time range, can be None if the file set has a *start_date*.
            In this case the *start_date* is used.
        last_date : str
            The last date of the time range, can be None if the file set has a *end_date*.
            In this case the *end_date* is used.
        """
        if first_date is None and self.start_date is None:
            raise ValueError("neither first_date nor start_date are given")
        d1 = self._as_date(first_date, self.start_date)

        if last_date is None and self.end_date is None:
            raise ValueError("neither last_date nor end_date are given")
        d2 = self._as_date(last_date, self.end_date)

        if d1 > d2:
            raise ValueError("start date '%s' is after end date '%s'" % (d1, d2))

        return [self._resolve(d1 + timedelta(days=x)) for x in range((d2-d1).days + 1)]

    @staticmethod
    def _as_date(d: Union[str, date], default) -> date:
        if d is None:
            return default
        if isinstance(d, str):
            return datetime.strptime(d, "%Y-%m-%d").date()
        if isinstance(d, date):
            return d
        raise ValueError

    def _resolve(self, date: date):
        path = self.full_pattern
        if "{YYYY}" in path:
            path = path.replace("{YYYY}", "%04d" % (date.year))
        if "{MM}" in path:
            path = path.replace("{MM}", "%02d" % (date.month))
        if "{DD}" in path:
            path = path.replace("{DD}", "%02d" % (date.day))
        return path


class FileSetCatalogue:
    def __init__(self, root_dir: str, fileset_types: Sequence[FileSetType]):
        self._fileset_types = fileset_types
        self._root_dir= root_dir

    @property
    def root_dir(self) -> str:
        return self._root_dir

    @property
    def fileset_types(self) -> Sequence[FileSetType]:
        return self._fileset_types


def fileset_types_from_json(json_str) -> Sequence[FileSetType]:
    as_dict = json.loads(json_str)
    fsts = []
    for fsd in as_dict:
        fsts.append(FileSetType(
            fsd['name'],
            fsd['base_dir'],
            fsd['start_date'],
            fsd['end_date'],
            fsd['num_files'],
            fsd['size_mb'],
            fsd['file_pattern'],
        ))
    return fsts


def fileset_cat_from_file(filename: str, root_dir: str) -> FileSetCatalogue:
    with open(filename) as json_file:
        json = json_file.read()
    fileset_types = fileset_types_from_json(json)
    return FileSetCatalogue(root_dir, fileset_types)
