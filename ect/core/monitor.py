"""

This module defines the :py:class:`Monitor` interface that may be used by functions and methods
that offer support for observation and control of long-running tasks.

Example usage:::
    from ect.core.util.monitor import starting

    def long_running_task(a, b, c, monitor):
        with monitor.starting('doing a long running task', total_work=100)
            # do 30% of the work here
            monitor.progress(work=30)
            # do 70% of the work here
            monitor.progress(work=70)

If your function makes calls to other functions that also support a monitor, use a *child-monitor*:::

    def long_running_task(a, b, c, monitor):
        with monitor.starting('doing a long running task', total_work=100)
            # let other_task do 30% of the work
            other_task(a, b, c, monitor=monitor.child(work=30))
            # let other_task do 70% of the work
            other_task(a, b, c, monitor=monitor.child(work=70))


The module also provides a simple but still useful default implementation :py:class:`ConsoleMonitor`, which
prints progress output directly to the console.

"""
import signal
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager


class Monitor(metaclass=ABCMeta):
    """
    A monitor is used to both observe and control a running operation.
    Pass ``Monitor.NULL`` to functions that expect a monitor instead of passing ``None``.

    The ``Monitor`` class is an abstract base class and clients must implement the following three abstract methods:
    :py:meth:`start`, :py:meth:`progress`, and :py:meth:`done`.

    """

    #: A valid monitor that effectively does nothing. Use ``Monitor.NULL`` it instead of passing ``None`` to
    #: functions and methods that expect an argument of type ``Monitor``.
    NULL = None

    @contextmanager
    def starting(self, label: str, total_work: float = None):
        """
        A context manager for easier use of progress monitors.
        Calls the monitor's ``start`` method with *label* and *total_work*.
        Will then take care of calling :py:meth:`Monitor.done`.

        :param monitor: The monitor
        :param label: Passed to the monitor's ``start`` method
        :param total_work: Passed to the monitor's ``start`` method
        :return:
        """
        self.start(label, total_work=total_work)
        try:
            yield self
        finally:
            self.done()

    @abstractmethod
    def start(self, label: str, total_work: float = None):
        """
        Call to signal that a task has started.

        :param label: A task label
        :param total_work: The total amount of work
        """

    @abstractmethod
    def progress(self, work: float = None, msg: str = None):
        """
        Call to signal that a task has mode some progress.

        :param work: The incremental amount of work.
        :param msg: A detail message.
        """

    @abstractmethod
    def done(self):
        """
        Call to signal that a task has been done.
        """

    def child(self, work: float):
        """
        Return a child monitor for the given partial amount of *work*.

        :param work: The partial amount of work.
        :return: a sub-monitor
        """
        return ChildMonitor(self, work)

    def cancel(self):
        """
        Request the task to be cancelled.
        This method will be usually called from the code that created the monitor, not by users of the monitor.
        For example, a GUI could create the monitor due to an invocation of a long-running task, and then
        the user wishes to cancel that task.
        The default implementation does nothing.
        Override to implement something useful.
        """
        pass

    def is_cancelled(self) -> bool:
        """
        Check if there is an external request to cancel the current task observed by this monitor.

        Users of a monitor shall frequently call this method and check its return value.
        If cancellation is requested, they should politely exit the current processing in a proper way, e.g.
        by cleaning up allocated resources.
        The default implementation returns ``False``.
        Subclasses shall override this method to return ``True`` if a task cancellation request was detected.

        :return: ``True`` if task cancellation was requested externally. The default implementation returns ``False``.
        """
        return False


class _NullMonitor(Monitor):
    def __repr__(self):
        # Overridden to make Sphinx use a readable name.
        return 'Monitor.NULL'

    def start(self, label: str, total_work: float = None):
        pass

    def progress(self, work: float = None, msg: str = None):
        pass

    def done(self):
        pass

    def child(self, partial_work: float):
        return Monitor.NULL


#: Pass ``Monitor.NULL`` to functions that expect a monitor instead of passing ``None``.
Monitor.NULL = _NullMonitor()


class ChildMonitor(Monitor):
    """
    A child monitor is responsible for a partial amount of work of a *parent_monitor*.


    :param parent_monitor: the parent monitor
    :param partial_work: the partial amount of work of *parent_monitor*.
    """

    def __init__(self, parent_monitor: Monitor, partial_work: float):
        self._parent_monitor = parent_monitor
        self._partial_work = partial_work
        self._total_work = None
        self._label = None

    def start(self, label: str, total_work: float = None):
        if not label:
            raise ValueError('label must be given')
        self._label = label
        self._total_work = total_work
        parent_work = 0.0 if total_work is not None else None
        self._parent_monitor.progress(work=parent_work, msg=self._label)

    def progress(self, work: float = None, msg: str = None):
        parent_work = self._partial_work * (work / self._total_work) if work is not None else None
        parent_msg = '%s: %s' % (self._label, msg) if msg is not None else None
        self._parent_monitor.progress(work=parent_work, msg=parent_msg)

    def done(self):
        parent_work = 0.0 if self._total_work is not None else None
        self._parent_monitor.progress(work=parent_work, msg=self._label)

    def cancel(self):
        self._parent_monitor.cancel()

    def is_cancelled(self) -> bool:
        return self._parent_monitor.is_cancelled()


class ConsoleMonitor(Monitor):
    """
    A simple console monitor that directly writes to stdout and detects cancellation requests via CTRL+C.
    """

    def __init__(self):
        self._label = None
        self._worked = None
        self._total_work = None
        self._cancelled = False
        self._old_ctrl_c_handler = False
        self._last_percentage = None

    def start(self, label: str, total_work: float = None):
        if not label:
            raise ValueError('label must be given')
        self._label = label
        self._worked = 0.
        self._total_work = total_work
        line = '%s: start' % self._label
        self.write_line(line)
        self._old_ctrl_c_handler = signal.signal(signal.SIGINT, self._on_ctrl_c)

    def progress(self, work: float = None, msg: str = None):
        percentage = None
        if work is not None:
            self._worked += work
            percentage = int(100. * self._worked / self._total_work + 0.5)
        # only display progress on integer percentage change
        if percentage != self._last_percentage:
            if msg is not None and percentage is not None:
                line = '%s: %d%%: %s' % (self._label, percentage, msg)
            elif percentage is not None:
                line = '%s: %d%%' % (self._label, percentage)
            elif msg:
                line = '%s: %s' % (self._label, msg)
            else:
                line = '%s: progress' % self._label
            self.write_line(line)
            self._last_percentage = percentage

    def done(self):
        line = '%s: done' % self._label
        self.write_line(line)
        signal.signal(signal.SIGINT, self._old_ctrl_c_handler)

    # noinspection PyMethodMayBeStatic
    def write_line(self, line):
        print(line)

    def cancel(self):
        self._cancelled = True

    def is_cancelled(self) -> bool:
        return self._cancelled

    # noinspection PyUnusedLocal,PyShadowingNames
    def _on_ctrl_c(self, signal, frame):
        self.cancel()
