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

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

import json
import time

import tornado.websocket

from cate.util import Monitor


class JsonRcpWebSocketMonitor(Monitor):
    """
    A Monitor implementation that reports progress as non-standard JSON-RCP messages of the form:

    {
        jsonrpc: "2.0",
        id: <int>,
        progress: {
            label: <str>,
            message: <str>,
            total: <float>,
            worked: <float>,
        }
    }

    :param method_id: The JSON-RCP method id
    :param handler: The Tornado WebSocket handler
    :param report_defer_period: The time in seconds between two subsequent progress reports
    """

    def __init__(self,
                 method_id: int,
                 handler: tornado.websocket.WebSocketHandler,
                 report_defer_period: float = None):
        self.method_id = method_id
        self.handler = handler
        self.report_defer_period = report_defer_period or 0.5
        self._cancelled = False
        self.last_time = None

        self.label = None
        self.total = None
        self.worked = None

    def _write_progress(self, message: str = None):
        current_time = time.time()
        if not self.last_time or (current_time - self.last_time) >= self.report_defer_period:

            progress = {}
            if self.label is not None:
                progress['label'] = self.label
            if message is not None:
                progress['message'] = message
            if self.total is not None:
                progress['total'] = self.total
            if self.worked is not None:
                progress['worked'] = self.worked

            self.handler.write_message(json.dumps(dict(jsonrpc="2.0",
                                                       id=self.method_id,
                                                       progress=progress)))
            self.last_time = current_time

    def start(self, label: str, total_work: float = None):
        self.label = label
        self.total = total_work
        self.worked = 0.0 if total_work else None
        self._write_progress(message='Started')
        # first progress method should always be sent
        self.last_time = None

    def progress(self, work: float = None, msg: str = None):
        if work:
            self.worked = (self.worked or 0.0) + work
        self._write_progress(message=msg)

    def done(self):
        self.worked = self.total
        self._write_progress(message='Done')

    def cancel(self):
        self._cancelled = True

    def is_cancelled(self) -> bool:
        return self._cancelled
