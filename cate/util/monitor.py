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

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

_DEBUG_DASK_PROGRESS = False
_DaskMonitor = None
_IS_DASK_AVAILABLE = None


class Cancellation(Exception):
    """The operations was cancelled by the user."""
    pass


class Monitor(metaclass=ABCMeta):
    """
    A monitor is used to both observe and control a running task.

    The ``Monitor`` class is an abstract base class for concrete monitors.
    Derived classes must implement the following three abstract methods:
    :py:meth:`start`, :py:meth:`progress`, and :py:meth:`done`.
    Derived classes must implement also the following two abstract methods, if they want cancellation support:
    :py:meth:`cancel` and :py:meth:`is_cancelled`.

    Pass ``Monitor.NONE`` to functions that expect a monitor instead of passing ``None``.

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

    #: A valid monitor that effectively does nothing. Use ``Monitor.NONE`` it instead of passing ``None`` to
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

    @contextmanager
    def observing(self, label: str):
        """
        A context manager for easier use of progress monitors.
        Observes a ``dask`` task and reports back to the monitor.

        :param label: Passed to the monitor's ``start`` method
        :return:
        """
        dask_monitor = _get_dask_monitor()
        if dask_monitor is not None:
            with dask_monitor(label=label, monitor=self):
                yield
        else:
            raise NotImplementedError('Monitor.observing() requires "dask" package to be installed')

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

    def child(self, work: float = 1) -> 'Monitor':
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

    def check_for_cancellation(self):
        """
        Checks if the monitor has been cancelled and raises a ``Cancellation`` in that case.
        """
        if self.is_cancelled():
            raise Cancellation()


# noinspection PyAbstractClass
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


# noinspection PyAbstractClass
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


# noinspection PyAbstractClass
class ConsoleMonitor(Monitor):
    """
    A simple console monitor that directly writes to ``sys.stdout`` and detects user cancellation requests via CTRL+C.

    :param stay_in_line: If ``True``, the text written out will stay in the same line.
    :param progress_bar_size: If ``> 1``, a progress monitor of max. *progress_bar_size* characters
        will be written to the console.
    """

    def __init__(self, stay_in_line=False, progress_bar_size=1):
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

    def __del__(self):
        self._register_ctrl_c_handler(self._old_ctrl_c_handler)

    def start(self, label: str, total_work: float = None):
        self.check_for_cancellation()
        if not label:
            raise ValueError('label must be given')
        self._label = label
        self._worked = 0.
        self._percentage = None
        self._total_work = total_work
        self._register_ctrl_c_handler(self._on_ctrl_c)
        # if self._stay_in_line:
        #    sys.stdout.write('\n')
        self._report_progress(msg='started')

    def progress(self, work: float = None, msg: str = None):
        self.check_for_cancellation()
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
        self.check_for_cancellation()
        self._register_ctrl_c_handler(self._old_ctrl_c_handler)
        if self.is_cancelled():
            self._report_progress(msg='cancelled')
        else:
            self._report_progress(msg='done' if self._msg is None else self._msg)
        if self._stay_in_line:
            sys.stdout.write('\n')

    def cancel(self):
        self._cancelled = True

    def is_cancelled(self) -> bool:
        return self._cancelled

    def _report_progress(self, percentage=None, msg=None):
        term_size = get_terminal_size().columns
        if sys.platform == 'win32':
            term_size -= 1

        label_text = '%s: ' % self._label
        percentage_text = '' if percentage is None else '%3d%% ' % percentage
        progress_bar_text = ''
        message_text = 'in progress...' if (not msg and percentage is None) else (msg or '')

        progress_bar_size = self._progress_bar_size
        min_text_size = len(label_text) + len(percentage_text) + len(message_text)
        if percentage is not None and progress_bar_size > 1:
            available_progress_bar_size = term_size - (min_text_size + 3)
            if available_progress_bar_size > 1:
                if progress_bar_size > available_progress_bar_size:
                    progress_bar_size = available_progress_bar_size
            else:
                progress_bar_size = 0
            if progress_bar_size > 1:
                done_count = int(progress_bar_size * percentage / 100 + 0.5)
                remaining_count = progress_bar_size - done_count
                progress_bar_text = '[%s%s] ' % ('#' * done_count, '-' * remaining_count)

        line = label_text + progress_bar_text + percentage_text + message_text

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

    # noinspection PyUnusedLocal,PyShadowingNames
    def _on_ctrl_c(self, signal, frame):
        self.cancel()

    # noinspection PyMethodMayBeStatic
    def _register_ctrl_c_handler(self, ctrl_c_handler):
        if ctrl_c_handler:
            try:
                signal.signal(signal.SIGINT, ctrl_c_handler)
            except ValueError:
                # If not on main thread, we may receive ValueError: signal only works in main thread
                pass


def _get_dask_monitor():
    global _DaskMonitor
    global _IS_DASK_AVAILABLE

    if _DaskMonitor is None and _IS_DASK_AVAILABLE is None:
        try:
            # noinspection PyUnresolvedReferences,PyPackageRequirements
            from dask.callbacks import Callback

            _IS_DASK_AVAILABLE = True

            class _DaskMonitor(Callback):
                """
                A ``dask.Callback`` that reports the task level notification that the
                dask scheduler generates to the provided ``Monitor``.

                This allows for tracking then progress inside dask compute/get calls and
                the possibility to cancel them.
                """

                def __init__(self, label: str, monitor: Monitor):
                    super().__init__()
                    self._label = label
                    self._monitor = monitor
                    self._is_done = False

                # noinspection PyUnusedLocal
                def _start_state(self, dsk, state):
                    if self._is_done:
                        return
                    num_tasks = sum(len(state[k]) for k in ['ready', 'waiting'])
                    self._monitor.start(label=self._label, total_work=num_tasks)
                    if _DEBUG_DASK_PROGRESS:
                        print("DaskMonitor.start_state: num_tasks=", num_tasks)

                # noinspection PyUnusedLocal
                def _posttask(self, key, result, dsk, state, worker_id):
                    if self._is_done:
                        return
                    self._monitor.progress(work=1)
                    if _DEBUG_DASK_PROGRESS:
                        print("DaskMonitor.posttask: key=", key)

                # noinspection PyUnusedLocal
                def _finish(self, dsk, state, failed):
                    if self._is_done:
                        return
                    self._monitor.done()
                    self._is_done = True
                    if _DEBUG_DASK_PROGRESS:
                        print("DaskMonitor.finish")

        except ImportError:
            _IS_DASK_AVAILABLE = False

    return _DaskMonitor
