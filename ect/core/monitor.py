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
