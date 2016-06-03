"""
The ``ds`` package comprises all specific data source implementations.
"""


def ect_init():
    from .esa_cci_portal_ftp import add_default_file_catalogue
    add_default_file_catalogue()
