# The MIT License (MIT)
# Copyright (c) 2016 by the ECT Development Team and contributors
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

Provides random utility functions.

*Implementation note: this module shall not have any dependencies to higher-level ECT modules.*

Verification
============

The module's unit-tests are located in
`test/test_util.py <https://github.com/CCI-Tools/ect-core/blob/master/test/test_util.py>`_ and may be executed using
``$ py.test test/test_util.py --cov=ect/core/util.py`` for extra code coverage information.

Components
==========
"""
import sys
import urllib.parse
from collections import OrderedDict
from contextlib import contextmanager
from datetime import datetime, date, timedelta
from io import StringIO
from typing import Union, Tuple


class _Undefined:
    def __str__(self):
        return "UNDEFINED"

    def __repr__(self):
        return "UNDEFINED"


#: Value used to indicate an undefined state.
UNDEFINED = _Undefined()


class Namespace:
    """
    A dictionary-like object that has dynamic attributes.

    Instances of the ``Namespace`` class have some similarities with JavaScript associative arrays; you can use
    string keys to create new attributes and use a string key as an attribute name later. At the same time, you
    can determine the length of the object and use integer indices as well as slices to access values.
    A ``Namespace`` remembers the order of attributes added by utilizing a ``collections.OrderedDict``.

    Constraints and properties of the ``Namespace`` object:

    * The ``Namespace`` class does not defines any methods on its own in order to avoid naming clashes with added keys.
    * All keys must be string that are valid Python names. Values may be of any type.
    * The order of attributes added is preserved.
    * Other than a dictionary, which returns a keys iterator, a ``Namespace`` iterator returns key-value pairs.

    Examples:

    >>> ns = Namespace()
    >>> ns['a'] = 1
    >>> ns['z'] = 2
    >>> len(ns.a)
    2
    >>> ns.a
    1
    >>> ns['z']
    2
    >>> ns[0]
    1
    >>> ns[:]
    [1, 2]
    >>> ns = Namespace([('a', 1), ('z', 2)])
    >>> list(ns)
    [('a', 1), ('z', 2)]

    :param items: sequence of (attribute-name, attribute-value) pairs
    """

    def __init__(self, items=list()):
        attributes = OrderedDict()
        for name, value in items:
            attributes[name] = value
        object.__setattr__(self, '_attributes', attributes)

    def __contains__(self, key):
        attributes = object.__getattribute__(self, '_attributes')
        return key in attributes

    def __len__(self):
        attributes = object.__getattribute__(self, '_attributes')
        return len(attributes)

    def __iter__(self):
        attributes = object.__getattribute__(self, '_attributes')
        return iter(attributes.items())

    def __setitem__(self, key, value):
        attributes = object.__getattribute__(self, '_attributes')
        if isinstance(key, int):
            key = list(attributes.keys())[key]
        attributes[key] = value

    def __getitem__(self, key):
        attributes = object.__getattribute__(self, '_attributes')
        if isinstance(key, int) or isinstance(key, slice):
            return list(attributes.values())[key]
        return attributes[key]

    def __delitem__(self, key):
        attributes = object.__getattribute__(self, '_attributes')
        if isinstance(key, int) or isinstance(key, slice):
            key = tuple(attributes.keys())[key]
        del attributes[key]

    def __setattr__(self, name, value):
        attributes = object.__getattribute__(self, '_attributes')
        attributes[name] = value

    def __getattr__(self, name):
        attributes = object.__getattribute__(self, '_attributes')
        if name in attributes:
            return attributes[name]
        else:
            raise AttributeError("attribute '%s' not found" % name)

    def __delattr__(self, name):
        attributes = object.__getattribute__(self, '_attributes')
        if name in attributes:
            del attributes[name]
        else:
            raise AttributeError("attribute '%s' not found" % name)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        attributes = object.__getattribute__(self, '_attributes')
        if len(attributes) == 0:
            return 'Namespace()'
        return 'Namespace(%s)' % repr(list(attributes.items()))


def extend(target_class, property_name=None, property_doc=None):
    """
    Return a class decorator for classes that will become extensions to the given *target_class*.
    The *target_class* will be extended by a new property with the given *name* and the given *doc*.
    The new property will return an instance of the decorated extension class. The property value will be lazily
    created by calling the extension class' ``__init__`` method with the the *target_class* instance
    as only argument.

    Example:

    Let ``Model`` be an existing API class. Now another module wishes to extend the ``Model`` class by additional
    methods. This could be done by inheritance, but this will cause severe compatibility issues once the
    ``Model`` class evolves and break the composition-over-inheritance design principle. In addition,
    instantiation of the derived class must be performed explicitly. Instead, we want all ``Model`` instances to
    automatically include our new methods. Here is the code:::

        @extend(Model, 'my_ext')
        class MyModelExt:
            '''My Model extension''''

            def __init__(self, model):
                self.model = model

            def some_new_method(self, x):
                self.model.some_old_method()
                # ...

        # Model API users can now use the API extension without explicitly instantiating MyModelExt:
        model = Model()
        model.my_ext.some_new_method()

    :param target_class: A target class or sequence of target classes that will be extended.
    :param property_name: The name of the new property in the target class.
                          If ``None``, a name will be derived from the *extension_class*.
    :param property_doc: The docstring of the new property in the target class.
                         If ``None``, the doc-string will be taken from the *extension_class*, if any.
    :return: A decorator.
    """

    def decorator(extension_class):
        return _add_extension(target_class, extension_class, property_name=property_name, property_doc=property_doc)

    return decorator


def _add_extension(target_class, extension_class, property_name=None, property_doc=None):
    """
    Add an "extension" property with *property_name* to the *target_class*. The property will return an
    instance of *extension_class* whose ``__init__`` method will be called with the the *target_class*
    instance as only argument.

    Use this function to dynamically add extensions to existing classes in order to avoid inheritance.
    This function should be used through its decorator function :py:func:`extend`.

    :param target_class: A target class or sequence of target classes that will be extended.
    :param extension_class: The class that implements the extension.
    :param property_name: The name of the new property in the target class.
                          If ``None``, a name will be derived from the *extension_class*.
    :param property_doc: The docstring of the new property in the target class.
                         If ``None``, the doc-string will be taken from the *extension_class*, if any.
    :return: The *extension_class*.
    """

    if not property_name:
        # generate a property name from extension class name
        property_name = []
        last_was_lower = False
        for c in extension_class.__name__:
            if last_was_lower and c.isupper():
                property_name.append('_')
            property_name.append(c.lower())
            last_was_lower = c.islower()
        property_name = ''.join(property_name)
    attribute_name = '_' + property_name

    # define a property getter that lazily creates the extension instance
    def _lazy_extension_getter(self):
        if hasattr(self, attribute_name):
            extension = getattr(self, attribute_name)
        else:
            extension = extension_class(self)
            setattr(self, attribute_name, extension)
        return extension

    # derive docstring for property
    doc = property_doc if property_doc else getattr(extension_class, '__doc__', None)

    # inject new property into all target classes
    try:
        iterator = iter(target_class)
    except TypeError:
        iterator = iter([target_class])
    for cls in iterator:
        if hasattr(cls, property_name):
            raise ValueError("an attribute with name '%s' already exists in %s", property_name, cls)
        setattr(cls, property_name, property(fget=_lazy_extension_getter, doc=doc))

    return extension_class


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


def object_to_qualified_name(value, fail=False, default_module_name='builtins') -> str:
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
    return None


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
    try:
        return [dtype(item) for item in value]
    except:
        return [dtype(value)]