"""
The ``ds`` package comprises all specific data source implementations.

This is a plugin package automatically imported by the installation script's entry point ``ect_ds``
(see the projects ``setup.py`` file).
"""


def ect_init():
    """Plugin initializer.
    Sets the default data store."""

    from .esa_cci_ftp import set_default_data_store
    set_default_data_store()
