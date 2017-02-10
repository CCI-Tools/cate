# The MIT License (MIT)
# Copyright (c) 2017 by the Cate Development Team and contributors
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

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

import os.path

DEFAULT_DATA_DIR_NAME = '.cate'
DEFAULT_DATA_PATH = os.path.join(os.path.expanduser('~'), DEFAULT_DATA_DIR_NAME)
DEFAULT_CONF_FILE = os.path.join(DEFAULT_DATA_PATH, 'conf.py')

LOCAL_CONF_FILE = 'cate-conf.py'

SCRATCH_WORKSPACES_DIR_NAME = 'scratch-workspaces'
SCRATCH_WORKSPACES_PATH = os.path.join(DEFAULT_DATA_PATH, SCRATCH_WORKSPACES_DIR_NAME)

WORKSPACE_CACHE_DIR_NAME = '.cate-cache'
WORKSPACE_DATA_DIR_NAME = '.cate-workspace'
WORKSPACE_WORKFLOW_FILE_NAME = 'workflow.json'

_ONE_MIB = 1024 * 1024
_ONE_GIB = 1024 * _ONE_MIB

#: Use a per-workspace file imagery cache, see REST "/res/tile/" API
WEBAPI_USE_WORKSPACE_IMAGERY_CACHE = False

# The number of bytes in a workspace's image file cache
WEBAPI_WORKSPACE_FILE_TILE_CACHE_CAPACITY = 1 * _ONE_GIB

# The number of bytes in a workspace's image in-memory cache
WEBAPI_WORKSPACE_MEM_TILE_CACHE_CAPACITY = 256 * _ONE_MIB

#: where the information about a running WebAPI service is stored
WEBAPI_INFO_FILE = os.path.join(DEFAULT_DATA_PATH, 'webapi.json')

#: where a running WebAPI service logs to
WEBAPI_LOG_FILE_PREFIX = os.path.join(DEFAULT_DATA_PATH, 'webapi.log')

#: allow a 100 ms period between two progress messages sent to the client
WEBAPI_PROGRESS_DEFER_PERIOD = 0.5

#: allow two minutes timeout for any synchronous workspace I/O
WEBAPI_WORKSPACE_TIMEOUT = 2 * 60.0

#: allow one hour timeout for any synchronous workflow resource processing
WEBAPI_RESOURCE_TIMEOUT = 60 * 60.0

#: allow one hour extra timeout for matplotlib to block the WebAPI service's main thread by showing a Qt window
WEBAPI_PLOT_TIMEOUT = 60 * 60.0

#: By default, WebAPI service will auto-exit after 2 hours of inactivity, if WebAPI auto-exit enabled
WEBAPI_ON_INACTIVITY_AUTO_STOP_AFTER = 120 * 60.0

#: By default, WebAPI service will auto-exit after 5 seconds if all workspaces are closed, if WebAPI auto-exit enabled
WEBAPI_ON_ALL_CLOSED_AUTO_STOP_AFTER = 5.0

