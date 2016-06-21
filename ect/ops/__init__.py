"""
The ``ops`` package comprises all specific operation and processor implementations.

This is a plugin package automatically imported by the installation script's entry point ``ect_ops``
(see the projects ``setup.py`` file).
"""

from .timeseries import timeseries

__all__ = ['timeseries', 'resampling']


def ect_init():
    pass
