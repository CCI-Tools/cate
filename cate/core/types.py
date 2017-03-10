
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

__author__ = "Janis Gailis (S[&]T Norway)"

"""
Description
===========

Implementation of custom complex types and appropriate validation routines

Use TYPE_ALIAS in method signature for static type checking, use
is_type(value, TYPE) as a replacement for the built-in isinstance() for
complex 'typing' based types.

For example::

@op
@op_input('name1', data_type=TYPE)
def some_op(name1: TYPE_ALIAS -> TypeNotation[TYPE]:
    # Do something useful
    pass

Or:
def some_method(name1: TYPE_ALIAS) -> bool:
    is_type(name1, TYPE)
    # Do something useful
    pass

To add a new type, add a new type definition, an alias and a type class
and add the new definition to TYPES.

"""
from abc import ABCMeta, abstractmethod
from typing import Union, Any, List

from cate.util.misc import to_list

# Using strings instead of ints for better debug message readability down the
# line
POLYGON = 'POLYGON'
POLYGON_ALIAS = Union[str, List[str]]

VARIABLE = 'VARIABLE'
VARIABLE_ALIAS = Union[str, List[str]]

TIME_SELECTION = 'TIME_SELECTION'
TIME_SELECTION_ALIAS = Union[str, List[str]]


class _ComplexType(metaclass=ABCMeta):
    """
    An abstract complex type
    """
    @staticmethod
    @abstractmethod
    def _is_type(val: Any) -> bool:
        """
        Determine if given value can be treated as belonging to this type

        :param val: Value to check
        """

    @classmethod
    @abstractmethod
    def _to_op_object(cls, val: Any) -> Any:
        """
        Convert the given value to an object that operations expect to use for
        this type

        :param val: Value to convert
        """


class _Polygon(_ComplexType):
    """
    Complex type for a Polygon object
    """
    @staticmethod
    def _is_type(val: Any) -> bool:
        return False

    @classmethod
    def _to_op_object(cls, val: Any) -> Any:
        return None


class _Variable(_ComplexType):
    """
    Complex type for a Variable object
    """
    @staticmethod
    def _is_type(val: Any) -> bool:
        if isinstance(val, str):
            # It's a single string, yay!
            return True

        if isinstance(val, list):
            # It's a list, yay!
            for item in val:
                if not isinstance(item, str):
                    # But at least one item is not a string, darn.
                    return False

            # It's a list of strings, yay!
            return True

        # It's something else completely
        return False

    @classmethod
    def _to_op_object(cls, val: VARIABLE_ALIAS) -> List[str]:
        if not cls._is_type(val):
            raise TypeError('Provided value is not of type {},'
                            ' {}'.format(VARIABLE, VARIABLE_ALIAS))
        return to_list(val)


class _TimeSelection(_ComplexType):
    """
    Complex type for a Time selection object
    """
    @staticmethod
    def _is_type(val: Any) -> bool:
        return False

    @classmethod
    def _to_op_object(val: Any) -> Any:
        return None


_TYPES = {POLYGON: _Polygon,
          VARIABLE: _Variable,
          TIME_SELECTION: _TimeSelection}


def is_type(value: Any, maybe_type: Any) -> bool:
    """
    Replacement for the built-in isinstance() that works with custom Cate
    types.

    :param value: Value to check
    :param maybe_type: type to check
    """
    try:
        return isinstance(value, maybe_type)
    except TypeError:
        if not isinstance(maybe_type, str):
            # maybe_type is not a string, probably we have a complex
            # constructed type using typing package Union or Sequence
            raise TypeError('Not possible to explicitly validate {}'
                            ' types. Have you forgotten to add data_value=TYPE'
                            ' in @op_input decorator?'.format(maybe_type))

    if maybe_type not in _TYPES:
        raise TypeError('Provided type definition {} not'
                        ' found'.format(maybe_type))
    # Now we should have a valid type definition

    return _TYPES[maybe_type]._is_type(value)

def to_op_object(value: Any, typ: Any) -> Any:
    """
    Return an object that operations expect to work with for the given type

    :param value: Value to process
    :param typ: Complex type
    """
    return _TYPES[typ]._to_op_object(value)
