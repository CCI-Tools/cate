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
from .timeseries import timeseries
from .io import load_dataset, save_dataset

__all__ = [
    'timeseries',
    'resample_2d',
    'downsample_2d',
    'upsample_2d',
    'load_dataset',
    'save_dataset'
]


def ect_init():
    """
    Plugin initializer.

    Left empty because operations are registered automatically via decorators.
    """
    pass
