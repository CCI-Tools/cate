"""
Description
===========

The ``ds`` package comprises all specific data source implementations.

This is a plugin package automatically imported by the installation script's entry point ``ect_ds``
(see the projects ``setup.py`` file).

Verification
============

The module's unit-tests are located

* `test/ds/test_esa_cci_ftp.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ds/test_esa_cci_ftp.py>`_.

and may be executed using ``$ py.test test/ops/test_<MODULE>.py --cov=ect/ops/<MODULE>.py`` for extra code coverage
information.

Components
==========
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
