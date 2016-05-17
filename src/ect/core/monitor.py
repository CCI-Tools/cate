class Monitor:
    """
    A monitor is used to both observe and control a running operation.
    Pass ``Monitor.NULL`` to ECT API functions that expect a monitor instead of passing ``None``.
    """

    #: A monitor that has no effect. Use instead of passing ``None``
    NULL = None


Monitor.NULL = Monitor()
