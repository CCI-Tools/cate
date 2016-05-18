"""
ECT core classes and functions.
"""

from .cdm import Dataset
from .cdm import DatasetCollection
from .cdm import DatasetOperations
from .cdm_shapefile import add_shapefile_dataset
from .cdm_xarray import add_xarray_dataset
from .monitor import Monitor
from .node import Node, Connector, Connection

DatasetCollection.add_xarray_dataset = add_xarray_dataset
DatasetCollection.add_shapefile_dataset = add_shapefile_dataset

__all__ = [
    'Dataset',
    'DatasetCollection',
    'DatasetOperations',
    'Monitor',
    'monitor',
    # 'add_op',
    # 'remove_op',
    # 'get_op',
    # 'op',
    # 'op_input',
    # 'op_output',
    # 'op_return',
    # 'Node',
]
