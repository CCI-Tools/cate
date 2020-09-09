# The MIT License (MIT)
# Copyright (c) 2020 by the ESA CCI Toolbox development team and contributors
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

import pandas as pd
import re
from typing import Optional
from typing import Tuple

_RE_TO_DATETIME_FORMATS = patterns = [(re.compile(14 * '\\d'), '%Y%m%d%H%M%S'),
                                      (re.compile(12 * '\\d'), '%Y%m%d%H%M'),
                                      (re.compile(8 * '\\d'), '%Y%m%d'),
                                      (re.compile(6 * '\\d'), '%Y%m'),
                                      (re.compile(4 * '\\d'), '%Y')]

def find_datetime_format(filename: str) -> Tuple[Optional[str], int, int]:
    for regex, time_format in _RE_TO_DATETIME_FORMATS:
        searcher = regex.search(filename)
        if searcher:
            p1, p2 = searcher.span()
            return time_format, p1, p2
    return None, -1, -1

def get_timestamp_from_string(string: str) -> pd.Timestamp:
    time_format, p1, p2 = find_datetime_format(string)
    if not time_format:
        return None
    try:
        return pd.to_datetime(string[p1:p2], format=time_format)
    except ValueError:
        return None

def get_timestamps_from_string(string: str) -> (pd.Timestamp, pd.Timestamp):
    first_time = None
    second_time = None
    time_format, p1, p2 = find_datetime_format(string)
    try:
        if time_format:
            first_time = pd.to_datetime(string[p1:p2], format=time_format)
        string_rest = string[p2:]
        time_format, p1, p2 = find_datetime_format(string_rest)
        if time_format:
            second_time = pd.to_datetime(string_rest[p1:p2], format=time_format)
    except ValueError:
        pass
    return first_time, second_time
