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

from collections import OrderedDict

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"


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
