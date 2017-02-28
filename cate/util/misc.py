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

import os
import os.path
import sys
import urllib.parse
from contextlib import contextmanager
from datetime import datetime, date, timedelta
from io import StringIO
from typing import Union, Tuple


def qualified_name_to_object(qualified_name: str, default_module_name='builtins'):
    """
    Convert a fully qualified name into a Python object.
    It is true that ``qualified_name_to_object(object_to_qualified_name(obj)) is obj``.

    >>> qualified_name_to_object('unittest.TestCase')
    <class 'unittest.case.TestCase'>

    See also :py:func:`object_to_qualified_name`.

    :param qualified_name: fully qualified name of the form [<module>'.'{<name>'.'}]<name>
    :param default_module_name: default module name to be used if the name does not contain one
    :return: the Python object
    :raise ImportError: If the module could not be imported
    :raise AttributeError: If the name could not be found
    """
    parts = qualified_name.split('.')

    if len(parts) == 1:
        module_name = default_module_name
    else:
        module_name = parts[0]
        parts = parts[1:]

    value = __import__(module_name)
    for name in parts:
        value = getattr(value, name)
    return value


def object_to_qualified_name(value, fail=False, default_module_name='builtins') -> Union[str, None]:
    """
    Get the fully qualified name of a Python object.
    It is true that ``qualified_name_to_object(object_to_qualified_name(obj)) is obj``.

    >>> from unittest import TestCase
    >>> object_to_qualified_name(TestCase)
    'unittest.case.TestCase'

    See also :py:func:`qualified_name_to_object`.

    :param value: some Python object
    :param fail: raise ``ValueError`` if name cannot be derived.
    :param default_module_name: if this is the *value*'s module name, no module name will be returned.
    :return: fully qualified name if it can be derived, otherwise ``None`` if *fail* is ``False``.
    :raise ValueError: if *fail* is ``True`` and the name cannot be derived.
    """

    module_name = value.__module__ if hasattr(value, '__module__') else None
    if module_name == default_module_name:
        module_name = None

    # Not sure, if '__qualname__' is the better choice - no Pythons docs available
    name = value.__name__ if hasattr(value, '__name__') else None
    if name:
        return module_name + '.' + name if module_name else name

    if fail:
        raise ValueError("missing attribute '__name__'")

    return str(value)


@contextmanager
def fetch_std_streams():
    """
    A context manager which can be used to temporarily fetch the standard output streams
    ``sys.stdout`` and  ``sys.stderr``.

    Usage:::

        with fetch_std_streams() as stdout, stderr
            sys.stdout.write('yes')
            sys.stderr.write('oh no')
        print('fetched', stdout.getvalue())
        print('fetched', stderr.getvalue())

    :return: yields  ``sys.stdout`` and  ``sys.stderr`` redirected into buffers of type ``StringIO``
    """
    sys.stdout.flush()
    sys.stderr.flush()

    old_stdout = sys.stdout
    old_stderr = sys.stderr

    sys.stdout = StringIO()
    sys.stderr = StringIO()

    try:
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout.flush()
        sys.stderr.flush()

        sys.stdout = old_stdout
        sys.stderr = old_stderr


def encode_url_path(path_pattern: str, path_args: dict = None, query_args: dict = None) -> str:
    """
    Return an URL path with an optional query string which is composed of a *path_pattern* that may contain
    placeholders of the form ``{name}`` which will be replaced by URL-encoded versions of the
    corresponding values in *path_args*, i.e. ``urllib.parse.quote_plus(path_args[name])``.
    An optional query string is composed of the URL-encoded key-value pairs given in *query_args*, i.e.
    ``urllib.parse.urlencode(query_args)``.

    :param path_pattern: The path pattern which may include any number of placeholders of the form ``{name}``
    :param path_args: The values for the placeholders in *path_pattern*
    :param query_args: The query arguments
    :return: an URL-encoded path
    """
    path = path_pattern
    if path_args:
        quoted_pattern_args = dict(path_args)
        for name, value in path_args.items():
            quoted_pattern_args[name] = urllib.parse.quote_plus(str(value)) if value is not None else ''
        path = path_pattern.format(**quoted_pattern_args)
    query_string = ''
    if query_args:
        query_string = '?' + urllib.parse.urlencode(query_args)
    return path + query_string


def to_datetime_range(start_datetime_or_str: Union[datetime, date, str, None],
                      end_datetime_or_str: Union[datetime, date, str, None],
                      default=None) -> Tuple[datetime, datetime]:
    if not start_datetime_or_str and not end_datetime_or_str:
        return default
    if not end_datetime_or_str:
        if not start_datetime_or_str:
            raise ValueError('start_datetime_or_str argument must be given')
        end_datetime_or_str = start_datetime_or_str
    start_datetime = to_datetime(start_datetime_or_str, upper_bound=False)
    end_datetime = to_datetime(end_datetime_or_str, upper_bound=True)
    return start_datetime, end_datetime


def to_datetime(datetime_or_str: Union[datetime, date, str, None], upper_bound=False, default=None) -> datetime:
    if datetime_or_str is None or datetime_or_str == '':
        return default
    elif isinstance(datetime_or_str, str):
        format_to_timedelta = [("%Y-%m-%d %H:%M:%S", timedelta()),
                               ("%Y-%m-%d", timedelta(hours=24, seconds=-1)),
                               ("%Y-%m", timedelta(weeks=4, seconds=-1)),
                               ("%Y", timedelta(days=365, seconds=-1)),
                               ]
        for f, td in format_to_timedelta:
            try:
                dt = datetime.strptime(datetime_or_str, f)
                return dt + td if upper_bound else dt
            except ValueError:
                pass
        raise ValueError('Invalid date/time value: "%s"' % datetime_or_str)
    elif isinstance(datetime_or_str, datetime):
        return datetime_or_str
    elif isinstance(datetime_or_str, date):
        return datetime(datetime_or_str.year, datetime_or_str.month, datetime_or_str.day, 12)
    else:
        raise TypeError('datetime_or_str argument must be a string or instance of datetime.date')


def to_list(value,
            dtype: type = str,
            name: str = None,
            nullable: bool = True,
            csv: bool = True,
            strip: bool = True):
    """
    Convert *value* into a list of items of type *dtype*.

    :param value: Some value that may be a sequence or a scalar
    :param dtype: The desired target type
    :param name: An (argument) name used for ``ValueError`` messages
    :param nullable: Whether *value* can be None.
    :param csv: Whether to split *value* if it is a string containing commas.
    :param strip: Whether to strip CSV string values, used only if *csv* is True.
    :return: A list with elements of type *dtype* or None if *value* is None and *nullable* is True
    """
    if value is None:
        if not nullable:
            raise ValueError('%s argument must not be None' % (name or 'some'))
        return value
    if csv and isinstance(value, str):
        items = value.split(',')
        return [dtype(item.strip() if strip else item) for item in items]
    if isinstance(value, dtype):
        return [value]
    # noinspection PyBroadException
    try:
        return [dtype(item) for item in value]
    except:
        return [dtype(value)]


_PYTHON_QUOTE_CHARS = ['"', "'"]


def to_str_constant(s: str, quote="'") -> str:
    """
    Convert a given string into another string that is a valid Python representation of a string constant.
    :param s: the string
    :param quote: the quote character, either a single or double quote
    :return:
    """
    if s is None:
        raise ValueError()
    if quote not in _PYTHON_QUOTE_CHARS:
        raise ValueError()
    return quote + s.replace('\\', '\\\\').replace(quote, "\\%s" % quote) + quote


def is_str_constant(s: str) -> bool:
    """
    Test whether a given string is a Python representation of a string constant.

    :param s: the string
    :return: True, if so.
    """
    return s and len(s) >= 2 and s[0] == s[-1] and s[0] in _PYTHON_QUOTE_CHARS


@contextmanager
def cwd(path: str):
    """
    A context manager which can be used to temporarily change the current working directory to *path*.

    Usage:::

        print(os.getcwd())
        with cwd('./test'):
            print(os.getcwd())
        print(os.getcwd())

    :return: yields the new working directory (absolute *path* passed in)
    """
    if path is None:
        raise ValueError('path argument must be given')

    old_dir = os.getcwd()

    try:
        os.chdir(path)
        yield os.getcwd()
    finally:
        os.chdir(old_dir)
