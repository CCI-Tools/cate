"""
The ``ds`` package comprises all specific data source implementations.
"""


def ect_init():
    from .esa_cci_portal_ftp import set_default_data_store
    set_default_data_store()
