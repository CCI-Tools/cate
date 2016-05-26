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

class DataSource:
    def __init__(self, name: str, glob: str):
        self._name = name
        self._glob = glob

    @property
    def name(self) -> str:
        return self._name

    @property
    def glob(self) -> str:
        return self._glob

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


def query_data_sources(catalogues: Union[Catalogue, Sequence[Catalogue]] = DEFAULT_CATALOGUE, **constraints) -> List[DataSource]:
    """
    Queries the catalogue(s) for data sources matching the given constrains.

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
    """
    Load and decode a dataset.

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
