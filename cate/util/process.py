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

import concurrent.futures
import platform
import re
import subprocess
import shlex
import time
from typing import Callable, Optional, Tuple, Union, Dict, Sequence

from . import Monitor

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"


def run_subprocess(command: Union[str, Sequence[str]],
                   cwd: Optional[str] = None,
                   env: Optional[Dict[str, str]] = None,
                   shell: bool = False,
                   started_handler: Optional[Callable[[subprocess.Popen], None]] = None,
                   stdout_handler: Optional[Callable[[str], None]] = None,
                   stderr_handler: Optional[Callable[[str], None]] = None,
                   done_handler: Optional[Callable[[int], None]] = None,
                   is_cancelled: Optional[Callable[[], bool]] = None,
                   cancelled_check_period: float = 0.1,
                   kill_on_cancel=False):
    """
    Execute a child program in a new process and wait for its termination.

    :param command: The command to be executed, may be a string or sequence of string arguments.
    :param cwd: Optional current working directory.
    :param env: Optional dictionary of environment variables.
    :param shell: Whether to use the shell as the program to execute.
    :param started_handler: An optional callable that is called with the program's process
           (a ``subprocess.Popen`` object)
    :param stdout_handler: An optional callable that receives the process' stdout as lines of UTF-8 encoded text.
    :param stderr_handler: An optional callable that receives the process' stderr as lines of UTF-8 encoded text.
    :param done_handler: An optional callable that is called with the program's exit code
    :param is_cancelled: An optional callable that is called to determine whether the program's process
           should be killed
    :param cancelled_check_period: The time to sleep between subsequent *is_cancelled()* calls.
           Defaults to 0.1 seconds.
    :param kill_on_cancel: Whether to send a SIGKILL rather than a SIGTERM signal when cancellation
           is requested (Unix only)
    :return: the program's return code (an `int`) or `None` if it could not be determined.
    """

    assert command, "command must be provided"

    if isinstance(command, str) and not shell:
        args = shlex.split(command, posix=platform.system() != 'Windows')
    else:
        args = command

    process = subprocess.Popen(args,
                               shell=shell,
                               cwd=cwd,
                               env=env,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               bufsize=0)

    if started_handler:
        started_handler(process)

    def _read_line(fp, handler):
        while process.returncode is None:
            line = fp.readline()
            if handler:
                # noinspection PyBroadException
                try:
                    handler(line.decode("utf-8"))
                except Exception:
                    # import traceback, sys
                    # traceback.print_exc(file=sys.stderr)
                    pass
            if not line:
                return
            if is_cancelled is not None and is_cancelled():
                _cancel(process, kill_on_cancel)

    def _read_line_stdout():
        _read_line(process.stdout, stdout_handler)

    def _read_line_stderr():
        _read_line(process.stderr, stderr_handler)

    def _check_cancelled():
        if is_cancelled is None:
            return
        while process.returncode is None:
            if is_cancelled():
                _cancel(process, kill_on_cancel)
            time.sleep(cancelled_check_period or 0.1)

    def _wait():
        return_code = process.wait()
        if done_handler:
            done_handler(return_code)
        return return_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        f1 = executor.submit(_read_line_stdout)
        f2 = executor.submit(_read_line_stderr)
        f3 = executor.submit(_check_cancelled)
        f4 = executor.submit(_wait)
        statuses = concurrent.futures.wait([f1, f2, f3, f4])
        result = f4.result() if f4 in statuses.done else None

    return result


def _cancel(process: subprocess.Popen, kill_on_cancel: bool):
    if kill_on_cancel:
        process.kill()
    else:
        process.terminate()
    return True


class ProcessOutputMonitor:
    """
    A stdout handler for :py:func:`execute` the delegates extracted progress information to a monitor.
    Information is extracted using regular expressions or a callable that extracts the information.

    :param monitor: The progress monitor to which extracted information from process outputs is delegated.
    :param label: A default label that is used in case no start label has be extracted
    :param total_work: Default total work that is used in case no total work could be determined
    :param started: Either a callable that receives a text line and returns a tuple (label, total_work)
           or a regex that must match in order to signal the start of progress monitoring.
           The regex must provide the group names "label" or "total_work" or both,
           e.g. "(?P<label>\w+)" or "(?P<total_work>\d+)"
    :param progress: Either a callable that receives a text line and returns a tuple (work, msg)
           or a regex that must match in order to signal process.
           The regex must provide group names "work" or "msg" or both,
           e.g. "(?P<msg>\w+)" or "(?P<work>\d+)"
    :param done: Either a callable that receives a text line and returns True or False
           or a regex that must match in order to signal the end of progress monitoring.
    """

    def __init__(self,
                 monitor: Monitor,
                 label: str = None,
                 total_work: float = None,
                 started: Union[str, Callable] = None,
                 progress: Union[str, Callable] = None,
                 done: Union[str, Callable] = None,
                 work_is_incremental=False):
        self.monitor = monitor
        self.label = label
        self.total_work = total_work
        self.current_work = 0
        self.work_is_incremental = work_is_incremental
        self.has_started = False
        self.is_done = False
        self.re_started = re.compile(started) if isinstance(started, str) else None
        self.re_progress = re.compile(progress) if isinstance(progress, str) else None
        self.re_done = re.compile(done) if isinstance(done, str) else None
        self.parse_started = started if callable(started) else None
        self.parse_progress = progress if callable(progress) else None
        self.parse_done = done if callable(done) else None

    def __call__(self, line: str):
        if self.is_done:
            return

        monitor = self.monitor

        is_done = self._do_parse_done(line)
        if is_done:
            if self.has_started:
                monitor.done()
            self.done = True
            return

        work, msg = self._do_parse_progress(line)
        if work or msg:
            if not self.has_started:
                monitor.start(self.label or 'Process running', total_work=self.total_work)
                self.has_started = True
            if work and not self.work_is_incremental:
                work_delta = work - self.current_work
                self.current_work += work_delta
                work = work_delta
            monitor.progress(work=work, msg=msg)

        if not self.has_started:
            label, total_work = self._do_parse_started(line)
            if label or total_work is not None:
                monitor.start(label or self.label or 'Process running', total_work=total_work or self.total_work)
                self.has_started = True

    def _do_parse_started(self, line: str) -> Tuple[Optional[str], Optional[float]]:
        label = None
        total_work = None
        if self.re_started:
            m = self.re_started.match(line)
            if m:
                d = m.groupdict()
                label = d.get('label')
                total_work = d.get('total_work')
                if total_work:
                    total_work = float(total_work)
        elif self.parse_started:
            label, total_work = self.parse_started(line)
        return label, total_work

    def _do_parse_progress(self, line: str) -> Tuple[Optional[float], Optional[str]]:
        work = None
        msg = None
        if self.re_progress:
            m = self.re_progress.match(line)
            if m:
                d = m.groupdict()
                work = d.get('work')
                if work:
                    work = float(work)
                msg = d.get('msg')
        elif self.parse_progress:
            work, msg = self.parse_progress(line)
        return work, msg

    def _do_parse_done(self, line: str) -> bool:
        if self.re_done:
            return self.re_done.match(line) is not None
        elif self.parse_done:
            return self.parse_done(line)
        else:
            return line == ''
