"""
This plugin module adds the ESA CCI Data Portal's FTP data source to
the data store registry and makes it the default data store.

Verification
============

The module's unit-tests are located in `test/ds/test_esa_cci_ftp.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ds/test_esa_cci_ftp.py>`_
and may be executed using ``$ py.test test/ds/test_esa_cci_ftp.py --cov=ect/ds/esa_cci_ftp.py`` for extra code coverage information.

Components
==========
"""

import os
import os.path
import pkgutil

from ect.core.io import FileSetDataStore, DATA_STORE_REGISTRY

DEFAULT_DATA_SOURCES_DIR = os.path.expanduser(os.path.join('~', '.ect', 'data_stores'))
DEFAULT_DATA_ROOT = os.path.join(DEFAULT_DATA_SOURCES_DIR, 'esa_cci_portal_ftp')


def set_default_data_store():
    """
    Defines the ESA CCI Data Portal's FTP data source and makes it the default data store.

    All data sources of the FTP data store are read from a JSON file ``esa_cci_ftp.json`` contained in this package.
    This JSON file has been generated from a scan of the entire FTP tree.
    """
    ect_data_root_dir = os.environ.get('ECT_DATA_ROOT', DEFAULT_DATA_ROOT)
    json_data = pkgutil.get_data('ect.ds', 'esa_cci_ftp.json')
    data_store = FileSetDataStore.from_json(ect_data_root_dir, json_data.decode('utf-8'))
    DATA_STORE_REGISTRY.add_data_store('default', data_store)
