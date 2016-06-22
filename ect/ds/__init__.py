"""
The ``ds`` package comprises all specific data source implementations.

This is a plugin package automatically imported by the installation script's entry point ``ect_ds``
(see the projects ``setup.py`` file).
"""

from .esa_cci_ftp import set_default_data_store

__all__ = [
    'set_default_data_store',
]


def ect_init():
    """
    Plugin initializer.
    Sets the default data store.
    """

    set_default_data_store()
