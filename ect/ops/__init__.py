"""
Description
===========

The ``ops`` package comprises all specific operation and processor implementations.

This is a plugin package automatically imported by the installation script's entry point ``ect_ops``
(see the projects ``setup.py`` file).

Verification
============

The module's unit-tests are located

* `test/ops/test_resample_2d.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ops/test_resample_2d.py>`_.
* `test/ops/test_downsample_2d.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ops/test_downsample_2d.py>`_.
* `test/ops/test_upsample_2d.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ops/test_upsample_2d.py>`_.
* `test/ops/test_timeseries.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ops/test_timeseries.py>`_.

and may be executed using ``$ py.test test/ops/test_<MODULE>.py --cov=ect/ops/<MODULE>.py`` for extra code coverage
information.

Components
==========
"""

from .resampling import resample_2d, downsample_2d, upsample_2d
from .timeseries import timeseries, timeseries_mean
from .harmonize import harmonize
from .filter import filter_dataset
from .coregistration import coregister
from .subset import subset_spatial, subset_temporal, subset_temporal_index
from .correlation import pearson_correlation

__all__ = [
    'timeseries',
    'timeseries_mean'
    'resample_2d',
    'downsample_2d',
    'upsample_2d',
    'harmonize',
    'filter_dataset',
    'coregister',
    'subset_spatial',
    'subset_temporal',
    'subset_temporal_index',
    'person_correlation'
]


def ect_init():
    """
    Plugin initializer.

    Left empty because operations are registered automatically via decorators.
    """
    pass
