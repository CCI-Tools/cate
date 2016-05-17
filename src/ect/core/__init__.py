"""
ECT core classes and functions.
"""

from .cdm import Dataset
from .cdm import DatasetCollection
from .cdm import DatasetOperations

from .cdm_xarray import add_xarray_dataset

DatasetCollection.add_xarray_dataset = add_xarray_dataset

__all__ = [
    'Dataset',
    'DatasetCollection',
    'DatasetOperations'
]