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

"""
Description
===========

The ``cate.core.plugin`` module exposes the Cate's plugin ``REGISTRY`` which is mapping from Cate entry point names to
plugin meta information. An Cate plugin is any callable in an internal/extension module registered with ``cate_plugins``
entry point.

Clients register a Cate plugin in the ``setup()`` call of their ``setup.py`` script. The following plugin example
comprises a main module ``cate_wavelet_gapfill`` which provides the entry point function ``cate_init``:::

    setup(
        name="cate-gapfill-wavelet",
        version="0.5",
        description='A wavelet-based gap-filling algorithm for the ESA CCI Toolbox',
        license='GPL 3',
        author='John Doe',
        packages=['cate_wavelet_gapfill'],
        entry_points={
            'cate_plugins': [
                'cate_wavelet_gapfill = cate_wavelet_gapfill:cate_init',
            ],
        },
        install_requires=['pywavelets >= 2.1'],
    )

The entry point callable should have the following signature::

    def cate_init(*args, **kwargs):
        pass

or::

    class EctInit:
        def __init__(*args, **kwargs)__:
            pass

The return values are ignored.

Verification
============

The module's unit-tests are located in `test/test_plugin.py <https://github.com/CCI-Tools/cate-core/blob/master/test/test_plugin.py>`_
and may be executed using ``$ py.test test/test_plugin.py --cov=cate/core/plugin.py`` for extra code coverage information.

Components
==========
"""

import sys
import traceback
from collections import OrderedDict

from pkg_resources import iter_entry_points


def _load_plugins():
    plugins = OrderedDict()
    for entry_point in iter_entry_points(group='cate_plugins', name=None):

        try:
            plugin = entry_point.load()
        except Exception:
            _report_plugin_exception(
                "unexpected exception while loading Cate plugin with entry point '%s'" % entry_point.name)
            continue

        if callable(plugin):
            try:
                plugin()
            except Exception:
                _report_plugin_exception(
                    "unexpected exception while executing Cate plugin with entry point '%s'" % entry_point.name)
                continue
        else:
            _report_plugin_error_msg("Cate plugin with entry point '%s' must be a callable but got a '%s'" % (
                entry_point.name, type(plugin)))
            continue

        # Here: use pkg_resources and introspection to generate a
        # JSON-serializable dictionary of plugin meta-information
        plugins[entry_point.name] = {'entry_point': entry_point.name}

    return plugins


def _report_plugin_error_msg(msg):
    sys.stderr.write("error: %s\n" % msg)


def _report_plugin_exception(msg):
    _report_plugin_error_msg(msg)
    print("-" * 80)
    traceback.print_exc(file=sys.stdout)
    print("-" * 80)


def cate_init(*arg, **kwargs):
    """
    No actual use, just demonstrates the signature of an Cate entry point callable.

    :param arg: any arguments (not used)
    :param kwargs: any keyword arguments (not used)
    :return: any or void (not used)
    """
    return arg, kwargs


#: Mapping of Cate entry point names to JSON-serializable plugin meta-information.
PLUGIN_REGISTRY = _load_plugins()
