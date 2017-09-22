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

The ``cate.util`` package provides application-independent utility functions.

This package is independent of other ``cate.*``packages and can therefore be used stand-alone.

Verification
============

The module's unit-tests are located in
`test/util <https://github.com/CCI-Tools/cate-core/blob/master/test/util>`_ and may be executed using
``$ py.test test/util --cov=cate/util`` for extra code coverage information.

Components
==========
"""

from .extend import extend
from .misc import *
from .monitor import Monitor, ChildMonitor, ConsoleMonitor, Cancellation
from .namespace import Namespace
from .opmetainf import OpMetaInfo
from .undefined import UNDEFINED
from .safe import safe_eval, get_safe_globals
from .process import run_subprocess, ProcessOutputMonitor
from .tmpfile import new_temp_file, del_temp_file, del_temp_files
from .opimpl import normalize_impl, adjust_temporal_attrs_impl, adjust_spatial_attrs_impl
