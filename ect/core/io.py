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
from typing import Sequence, Union
from ect.core import Dataset


class DataSource:
    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        return self._name


def _matches_filter(ds: DataSource, name_filter=None) -> bool:
        if name_filter and not name_filter in ds.name:
            return False
        return True


class Catalogue:
    def __init__(self, *data_sources: DataSource):
        self._data_sources = data_sources

    def filter(self, name_filter: None) -> [DataSource]:
        return [ds for ds in self._data_sources if _matches_filter(ds, name_filter=name_filter)]


DEFAULT_CATALOGUE = Catalogue(DataSource("aerosol"), DataSource("ozone"))


def query_data_sources(catalogues=DEFAULT_CATALOGUE, name_filter=None) -> Sequence[DataSource]:
    if isinstance(catalogues, Catalogue):
        catalogues = [catalogues]
    results = []
    for catalogue in catalogues:
        results.extend(catalogue.filter(name_filter=name_filter))
    return results


def open_dataset(data_source: Union[DataSource, str], constraints=None) -> Dataset:
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
    pass
