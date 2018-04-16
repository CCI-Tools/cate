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

import atexit
import logging
import os
import sys
import tempfile

_TEMP_FILES = []


def new_temp_file(**kwargs):
    """
    Delegates to standard Python ``tempfile.mkstemp(**kwargs)`` function and
    stores the returned pair in a global list files to be removed when the
    interpreter shuts down.
    :param kwargs: Same as for ``tempfile.mkstemp()``
    :return: a pair (fd, file) as returned by ``tempfile.mkstemp()``
    """
    pair = tempfile.mkstemp(**kwargs)
    _TEMP_FILES.append(pair)
    return pair


def del_temp_file(file):
    """
    Removes an individual file or pair (fd, file) registered by :py:func:`new_temp_file`.
    :param file: The file path or pair (fd, file) as returned by :py:func:`new_temp_file`.
    """
    global _TEMP_FILES

    try:
        _, file = file
    except ValueError:
        pass

    for i in range(len(_TEMP_FILES)):
        pair = _TEMP_FILES[i]
        if os.path.samefile(pair[1], file):
            is_deleted = _del_file(pair)
            if is_deleted:
                del _TEMP_FILES[i]
            return is_deleted
    return False


def del_temp_files(force=False):
    """
    Removes all temporary files registered by :py:func:`new_temp_file`.
    This function is called as a shutdown hook of the Python interpreter.
    It may be called directly if temporary files consume lots of disk space.
    """
    global _TEMP_FILES
    remaining = list()
    for pair in list(_TEMP_FILES):
        if not _del_file(pair):
            remaining.append(pair)
    _TEMP_FILES = [] if force else remaining


# Register del_temp_files() as a shutdown hook.
atexit.register(del_temp_files)


def _del_file(pair: str) -> bool:
    fd, file = pair
    if os.path.isfile(file):
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.remove(file)
        except OSError:
            logging.exception('removing temporary file %s failed' % file)
        return not os.path.isfile(file)
    else:
        return True


def get_temp_files():
    """Return the list of temporary files as list of pairs (fd, file). For testing only."""
    global _TEMP_FILES
    return _TEMP_FILES
