"""
Module Description
==================

This module defines the :py:class:`Monitor` interface that is used by ECT to observe and control
operations that support it. It also provides a number of useful default implementations:

* :py:class:`ConsoleMonitor`: Prints progress output to the console.
* :py:class:`BufferedMonitor`: Buffers progress output as a string so that e.g. a remote service can pick it up.


Module Reference
================
"""


class Monitor:
    """
    A monitor is used to both observe and control a running operation.
    Pass ``Monitor.NULL`` to ECT API functions that expect a monitor instead of passing ``None``.
    """

    #: A monitor that has no effect. Use instead of passing ``None``
    NULL = None


class _NullMonitor(Monitor):
    def __repr__(self):
        return 'Monitor.NULL'


#: Pass ``Monitor.NULL`` to ECT API functions that expect a monitor instead of passing ``None``.
Monitor.NULL = _NullMonitor()


class ConsoleMonitor(Monitor):
    """A monitor that prints progress output to the console."""
    pass


class BufferedMonitor(Monitor):
    """A monitor that buffers progress output as a string so that e.g. a remote service can pick it up."""
    pass
