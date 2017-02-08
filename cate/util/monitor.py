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

This module defines the :py:class:`Monitor` interface that may be used by functions and methods
that offer support for observation and control of long-running tasks.

The module also provides a simple but still useful default implementation :py:class:`ConsoleMonitor`, which
prints progress output directly to the console.

Components
==========
"""
import signal
import sys
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from shutil import get_terminal_size


class Monitor(metaclass=ABCMeta):
    """
    A monitor is used to both observe and control a running task.

    The ``Monitor`` class is an abstract base class for concrete monitors.
    Derived classes must implement the following three abstract methods:
    :py:meth:`start`, :py:meth:`progress`, and :py:meth:`done`.
    Derived classes must implement also the following two abstract methods, if they want cancellation support:
    :py:meth:`cancel` and :py:meth:`is_cancelled`.

    Pass ``Monitor.NULL`` to functions that expect a monitor instead of passing ``None``.

    Given here is an example of how progress monitors should be used by functions:::

        def long_running_task(a, b, c, monitor):
            with monitor.starting('doing a long running task', total_work=100)
                # do 30% of the work here
                monitor.progress(work=30)
                # do 70% of the work here
                monitor.progress(work=70)

    If a function makes calls to other functions that also support a monitor, a *child-monitor* is used:::

        def long_running_task(a, b, c, monitor):
            with monitor.starting('doing a long running task', total_work=100)
                # let other_task do 30% of the work
                other_task(a, b, c, monitor=monitor.child(work=30))
                # let other_task do 70% of the work
                other_task(a, b, c, monitor=monitor.child(work=70))

    """

    #: A valid monitor that effectively does nothing. Use ``Monitor.NULL`` it instead of passing ``None`` to
    #: functions and methods that expect an argument of type ``Monitor``.
    NONE = None

    @contextmanager
    def starting(self, label: str, total_work: float = None):
        """
        A context manager for easier use of progress monitors.
        Calls the monitor's ``start`` method with *label* and *total_work*.
        Will then take care of calling :py:meth:`Monitor.done`.

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

        Note that *label* and *total_work* are not passed to ``__init__``, because they are usually not known at
        constructions time. It is the responsibility of the task to derive the appropriate values for these.


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

    def child(self, work: float = 1):
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
        return 'Monitor.NONE'

    def start(self, label: str, total_work: float = None):
        pass

    def progress(self, work: float = None, msg: str = None):
        pass

    def done(self):
        pass

    def child(self, work: float = 1):
        return Monitor.NONE


#: Pass ``Monitor.NONE`` to functions that expect a monitor instead of passing ``None``.
Monitor.NONE = _NullMonitor()


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
        self._label = label
        self._total_work = total_work
        parent_work = 0.0 if total_work is not None else None
        self._parent_monitor.progress(work=parent_work, msg=self._label)

    def progress(self, work: float = None, msg: str = None):
        parent_work = self._partial_work * (work / self._total_work) if work is not None else None
        parent_msg = '%s: %s' % (self._label, msg) if self._label and msg else None
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
    A simple console monitor that directly writes to ``sys.stdout`` and detects user cancellation requests via CTRL+C.

    :param stay_in_line: If ``True``, the text written out will stay in the same line.
    :param progress_bar_size: If ``> 0``, a progress monitor of *progress_bar_size* characters
        will be written to the console. If ``0 or less`` it will adapt size to console size, where ``0`` is maximum
        available space
    """

    def __init__(self, stay_in_line=False, progress_bar_size=None):
        self._stay_in_line = stay_in_line
        self._progress_bar_size = progress_bar_size
        self._effective_progress_size = 0
        self._label = None
        self._worked = None
        self._total_work = None
        self._percentage = None
        self._cancelled = False
        self._last_line_len = None
        self._old_ctrl_c_handler = False
        self._msg = None
        self._term_size = 0

    def start(self, label: str, total_work: float = None):
        if not label:
            raise ValueError('label must be given')
        self._label = label
        self._worked = 0.
        self._percentage = None
        self._total_work = total_work
        self._old_ctrl_c_handler = signal.signal(signal.SIGINT, self._on_ctrl_c)
        # if self._stay_in_line:
        #    sys.stdout.write('\n')
        self._report_progress(msg='started')

    def progress(self, work: float = None, msg: str = None):
        percentage = None
        if work is not None:
            self._worked += work
            percentage = self._calc_percentage()
        # only display progress on integer percentage change or message change
        if percentage != self._percentage or msg != self._msg:
            self._report_progress(percentage, msg)
        self._percentage = percentage
        self._msg = msg

    def done(self):
        signal.signal(signal.SIGINT, self._old_ctrl_c_handler)
        if self.is_cancelled():
            self._report_progress(msg='cancelled')
        else:
            self._report_progress(msg='done' if self._msg is None else self._msg)
        if self._stay_in_line:
            sys.stdout.write('\n')

    def _recalculate_effective_progress_size(self, term_size: int):
        self._term_size = term_size
        self._effective_progress_size = self._term_size - len(self._label) + self._progress_bar_size - 11

    def _report_progress(self, percentage=None, msg=None):

        percentage_str = ''
        progress_bar_str = ''

        term_size = get_terminal_size().columns
        if term_size is not self._term_size:
            self._term_size = term_size
            self._effective_progress_size = self._term_size - len(self._label) + self._progress_bar_size \
                if self._progress_bar_size is not None else 0 - 11
        if percentage is not None:
            percentage_str = '%3d%%' % percentage
            if self._progress_bar_size is not None and self._progress_bar_size <= 0:
                done_count = int(self._effective_progress_size * percentage / 100 + 0.5)
                remaining_count = self._effective_progress_size - done_count
                progress_bar_str = '[%s%s] ' % ('#' * done_count, '-' * remaining_count)

        if percentage is not None and msg:
            line = '%s: %s%s %s' % (self._label, progress_bar_str, percentage_str, msg)
        elif percentage is not None:
            line = '%s: %s%s' % (self._label, progress_bar_str, percentage_str)
        elif msg:
            line = '%s: %s' % (self._label, msg)
        else:
            line = '%s: progress' % self._label

        if self._stay_in_line:
            if len(line) > term_size:
                line = line[:term_size]
            else:
                line = line.ljust(term_size, ' ')
            sys.stdout.write('\r')
            sys.stdout.write(line)
        else:
            sys.stdout.write(line)
            sys.stdout.write('\n')
        sys.stdout.flush()

    def _calc_percentage(self):
        return round(100. * self._worked / self._total_work) \
            if self._worked is not None and self._total_work is not None else None

    def cancel(self):
        self._cancelled = True

    def is_cancelled(self) -> bool:
        return self._cancelled

    # noinspection PyUnusedLocal,PyShadowingNames
    def _on_ctrl_c(self, signal, frame):
        self.cancel()
