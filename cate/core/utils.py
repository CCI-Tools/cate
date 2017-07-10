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

__author__ = "Marco Zuehlke (Brockmann Consult GmbH)"

"""
Internal module that contains utility functions.
"""

from dask.callbacks import Callback
from cate.util import Monitor

_DEBUG_DASK_PROGRESS = False


class DaskMonitor(Callback):

    def __init__(self, label: str, monitor: Monitor):
        super().__init__()
        self._label = label
        self._monitor = monitor

    def _start_state(self, dsk, state):
        num_tasks = sum(len(state[k]) for k in ['ready', 'waiting'])
        self._monitor.start(label=self._label, total_work=num_tasks)
        if _DEBUG_DASK_PROGRESS:
            print("DaskMonitor.start_state: num_tasks=", num_tasks)

    def _pretask(self, key, dsk, state):
        if self._monitor.is_cancelled():
            raise InterruptedError

    def _posttask(self, key, result, dsk, state, worker_id):
        self._monitor.progress(work=1)
        if _DEBUG_DASK_PROGRESS:
            print("DaskMonitor.posttask: key=", key)


    def _finish(self, dsk, state, failed):
        self._monitor.done()
        if _DEBUG_DASK_PROGRESS:
            print("DaskMonitor.finish")
