# The MIT License (MIT)
# Copyright (c) 2016 by the ECT Development Team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
Description
===========

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

_DATA_SOURCES_DIR = os.path.expanduser(os.path.join('~', '.ect', 'data_stores'))
_DATA_ROOT = os.path.join(_DATA_SOURCES_DIR, 'esa_cci_ftp')


def set_default_data_store():
    """
    Defines the ESA CCI Data Portal's FTP data store and makes it the default data store.

    All data sources of the FTP data store are read from a JSON file ``esa_cci_ftp.json`` contained in this package.
    This JSON file has been generated from a scan of the entire FTP tree.
    """
    ect_data_root_dir = os.environ.get('ECT_DATA_ROOT', _DATA_ROOT)
    json_data = pkgutil.get_data('ect.ds', 'esa_cci_ftp.json')
    data_store = FileSetDataStore.from_json(ect_data_root_dir, json_data.decode('utf-8'))
    DATA_STORE_REGISTRY.add_data_store('default', data_store)
