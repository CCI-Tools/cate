"""
The ``ops`` package comprises all specific operation and processor implementations.

This is a plugin package automatically imported by the installation script's entry point ``ect_ops``
(see the projects ``setup.py`` file).
"""

from .resampling import resample_2d, downsample_2d, upsample_2d
from .timeseries import timeseries

__all__ = [
    'timeseries',
    'resample_2d',
    'downsample_2d',
    'upsample_2d',
]


def ect_init():
    """
    Plugin initializer.

    Left empty because operations are registered automatically via decorators.
    """
    pass
