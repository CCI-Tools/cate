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

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

"""
Description
===========

The ``cate.util.web`` package provides application-independent utility functions for implementing
`JSON-RCP <http://www.jsonrpc.org/specification>`_ web services using the
`Tornado <http://www.tornadoweb.org/en/stable/>`_ web server.

This package is independent of other ``cate.*``packages, but it depends on the external ``tornado`` package.

Verification
============

The module's unit-tests are located in
`test/util/web <https://github.com/CCI-Tools/cate-core/blob/master/test/util/web>`_ and may be executed using
``$ py.test test/util/web --cov=cate/util/web`` for extra code coverage information.

Components
==========
"""

from .jsonrpchandler import JsonRcpWebSocketHandler
from .jsonrpcmonitor import JsonRcpWebSocketMonitor
