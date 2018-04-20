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

This module provides Cate's WebAPI Stop executable.

To use the WebAPI executable, invoke the module file as a script, type ``python3 cate/webapi/stop.py [ARGS] [OPTIONS]``.
Type `python3 cate/webapi/stop.py --help`` for usage help.

Verification
============

The module's unit-tests are located in
`test/webapi <https://github.com/CCI-Tools/cate/blob/master/test/webapi>`_
and may be executed using ``$ py.test test/webapi --cov=cate/webapi``
for extra code coverage information.

Components
==========
"""

import sys

from cate.util.web.webapi import run_stop
from cate.version import __version__
from cate.webapi.service import SERVICE_NAME, SERVICE_TITLE

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"


def main(args=None) -> int:
    return run_stop(SERVICE_NAME,
                    'Stops a running {}'.format(SERVICE_TITLE),
                    __version__, args=args)


if __name__ == "__main__":
    sys.exit(main())
