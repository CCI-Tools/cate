# The MIT License (MIT)
# Copyright (c) 2021 by the ESA CCI Toolbox development team and contributors
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

import traceback
from typing import Optional

from ..misc import is_debug_mode

_DEBUG_MODE = is_debug_mode()


def is_debug_mode() -> bool:
    global _DEBUG_MODE
    return _DEBUG_MODE


def set_debug_mode(value: bool):
    """ For testing only """
    global _DEBUG_MODE
    _DEBUG_MODE = value


def log_debug(*args):
    global _DEBUG_MODE
    if _DEBUG_MODE:
        print('WEBSOCKET RPC DEBUG:', *args)


def exception_to_json(exc_info, method=None) -> dict:
    exc_type, exc_value, exc_tb = exc_info
    return dict(method=method,
                exception=_get_exception_name(exc_type),
                traceback=''.join(traceback.format_exception(exc_type, exc_value, exc_tb)))


def _get_exception_name(exc_type: type) -> Optional[str]:
    try:
        name = exc_type.__name__
    except AttributeError:
        return str(exc_type)

    try:
        module = exc_type.__module__
    except AttributeError:
        module = None

    if module and module != 'builtins':
        return '%s.%s' % (module, name)
    return name
