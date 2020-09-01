# The MIT License (MIT)
# Copyright (c) 2016, 2017 by the ESA CCI Toolbox development team and contributors
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

The ``ds`` package comprises all specific data source implementations.

This is a plugin package automatically imported by the installation script's entry point ``cate_ds``
(see the projects ``setup.py`` file).

Verification
============

The module's unit-tests are located in `test/ds <https://github.com/CCI-Tools/cate/blob/master/test/ds>`_ and may
be executed using ``$ py.test test/ops/test_<MODULE>.py --cov=cate/ops/<MODULE>.py`` for extra code coverage
information.

Components
==========
"""


def cate_init():
    # Plugin initializer.
    from cate.ds.local import get_data_store_path
    from cate.core.ds import DATA_STORE_REGISTRY
    from cate.core.ds import XcubeDataStore

    store_configs = {
        "local-store": {
            "store_id": "directory",
            "store_params": {
                "base_dir": f"{get_data_store_path()}",
            }
        },
        "cci-store": {
            "store_id": "cciodp"
        },
        "cds-store": {
            "store_id": "cds"
        }
    }

    for store_id in store_configs:
        DATA_STORE_REGISTRY.add_data_store(
            XcubeDataStore(
                store_configs[store_id],
                store_id
            )
        )
