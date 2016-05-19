"""
ECT core classes and functions.
"""

from .cdm import Dataset
from .cdm import DatasetCollection
from .cdm import DatasetOperations
from .monitor import Monitor

# Import mixin methods for DatasetCollection by importing the extension modules
from . import cdm_shapefile as _
from . import cdm_xarray as _

# As last step, run plugin registration by importing the plugin module
from .plugin import ect_init as _

del _

__all__ = """Dataset DatasetCollection DatasetOperations Monitor""".split()
