"""
ECT root package.
"""

from .version import __version__

from .core.cdm import DatasetCollection, Dataset
from .core.io import open_dataset, query_data_sources
