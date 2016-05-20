"""
Module Description
==================

The ``ect.core.plugin`` module exposes the ECT's plugin ``REGISTRY`` which is mapping from ECT entry point names to
plugin meta information. An ECT plugin is any callable in an internal/extension module registered with ``ect_plugins``
entry point.

Clients register a ECT plugin in the ``setup()``call of their ``setup.py`` script. The following plugin example
comprises a main module ``ect_wavelet_gapfill`` which provides the entry point function ``ect_init``:::

    setup(
        name="ect-gapfill-wavelet",
        version="0.5",
        description='A wavelet-based gap-filling algorithm for the ESA CCI Toolbox',
        license='GPL 3',
        author='John Doe',
        packages=['ect_wavelet_gapfill'],
        entry_points={
            'ect_plugins': [
                'ect_wavelet_gapfill = ect_wavelet_gapfill:ect_init',
            ],
        },
        install_requires=['pywavelets >= 2.1'],
    )

The entry point callable should have the following signature::

    def ect_init(*args, **kwargs):
        pass

or::

    class EctInit:
        def __init__(*args, **kwargs)__:
            pass

The return values are ignored.

Module Reference
================
"""

import sys
import traceback
from collections import OrderedDict

from pkg_resources import iter_entry_points


def _load_plugins():
    plugins = OrderedDict()
    for entry_point in iter_entry_points(group='ect_plugins', name=None):

        try:
            plugin = entry_point.load()
        except Exception:
            _report_plugin_exception(
                "unexpected exception while loading ECT plugin with entry point '%s'" % entry_point.name)
            continue

        if callable(plugin):
            try:
                plugin()
            except Exception:
                _report_plugin_exception(
                    "unexpected exception while executing ECT plugin with entry point '%s'" % entry_point.name)
                continue
        else:
            _report_plugin_error_msg("ECT plugin with entry point '%s' must be a callable but got a '%s'" % (
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


def ect_init(*arg, **kwargs):
    """
    No actual use, just demonstrates the signature of an ECT entry point callable.

    :param arg: any arguments (not used)
    :param kwargs: any keyword arguments (not used)
    :return: any or void (not used)
    """
    return arg, kwargs


#: Mapping of ECT entry point names to JSON-serializable plugin meta-information.
REGISTRY = _load_plugins()
