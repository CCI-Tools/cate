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

__author__ = "Janis Gailis (S[&]T Norway), " \
             "Norman Fomferra (Brockmann Consult GmbH), " \
             "Marco Zühlke (Brockmann Consult GmbH)"

"""
Description
===========

Implementation of custom complex types and appropriate value validation and conversion routines.

For example::

@op
@op_input('file', data_type=PathLike)
def some_op(file: PathLike.TYPE) -> bool:
    # Do something useful
    pass

"""

from abc import abstractclassmethod, ABCMeta
from datetime import datetime, date
from typing import Any, Generic, TypeVar, List, Union, Tuple

from shapely.geometry import Point, Polygon, box
from shapely.wkt import loads

from cate.util.misc import to_list, to_datetime_range

T = TypeVar('T')


class Like(Generic[T], metaclass=ABCMeta):
    """
    Base class for complex types which can convert a value of varying source types into a target type *T*.
    The varying source types are therefore *like* the target type *T*.
    """

    #: A type that represents the varying source types. This is usually a ``typing.Union`` instance which
    #: combines the varying source types.
    TYPE = None

    @classmethod
    def name(cls) -> str:
        """Return the name of the type."""
        return cls.__name__

    @classmethod
    def accepts(cls, value: Any) -> bool:
        """Return ``True`` if the given value can be converted into the target type *T*, ``False`` otherwise."""
        try:
            cls.convert(value)
            return True
        except ValueError:
            return False

    @abstractclassmethod
    def convert(cls, value: Any) -> T:
        """
        Convert the given source value (of type ``Like.TYPE``) into an instance of type *T*.
        @:raises ValueError if the conversion fails.
        """
        pass

    @classmethod
    def format(cls, value: T) -> str:
        """
        Convert the given source value of type *T* into a string.
        @:raises ValueError if the conversion fails.
        """
        return str(value)


VariableNames = List[str]


# ===== Like-derived types below =====

class VariableNamesLike(Like[VariableNames]):
    """
    Type class for Variable selection objects

    Accepts:
        1. a string 'pattern1, pattern2, pattern3'
        2. a list ['pattern1', 'pattern2', 'pattern3']

    Converts to a list of strings
    """
    TYPE = Union[VariableNames, str]

    @classmethod
    def convert(cls, value: Any) -> VariableNames:
        """
        Convert the given value to a list of variable name patterns.
        """
        if isinstance(value, str):
            return to_list(value)

        if not isinstance(value, list):
            raise ValueError('Variable name pattern can only be a string or a'
                             ' list of strings.')

        for item in value:
            if not isinstance(item, str):
                raise ValueError('Variable name pattern can only be a string'
                                 ' or a list of strings.')

        return value


class PointLike(Like[Point]):
    """
    Type class for geometric Point objects

    Accepts:
        1. a Shapely Point
        2. a string 'lon,lat'
        3. a tuple (lon, lat)

    Converts to a Shapely point
    """
    TYPE = Union[Point, str, Tuple[float, float]]

    @classmethod
    def convert(cls, value: Any) -> Point:
        try:
            if isinstance(value, Point):
                return value
            if isinstance(value, str):
                pair = value.split(',')
                return Point(float(pair[0]), float(pair[1]))
            return Point(value[0], value[1])
        except Exception:
            raise ValueError('cannot convert value <%s> to %s' % (value, cls.name()))

    @classmethod
    def format(cls, value: Point) -> str:
        return "%s, %s" % (value.x, value.y)


class PolygonLike(Like[Polygon]):
    """
    Type class for geometric Polygon objects

    Accepts:
        1. a Shapely Polygon
        2. a string 'min_lon, min_lat, max_lon, max_lat'
        3. a WKT string 'POLYGON ...'
        4. a list of coordinates [(lon, lat), (lon, lat), (lon, lat)]

    Converts to a valid shapely Polygon.
    """
    TYPE = Union[Polygon, str, List[Tuple[float, float]]]

    @classmethod
    def convert(cls, value: Any) -> Polygon:
        try:
            if isinstance(value, Polygon):
                if value.is_valid:
                    return value
            if isinstance(value, list):
                polygon = Polygon(value)
                if polygon.is_valid:
                    return polygon
            if isinstance(value, str):
                value = value.lstrip()
                if value[:7].lower() == 'polygon':
                    polygon = loads(value)
                else:
                    val = [float(x) for x in value.split(',')]
                    polygon = box(val[0], val[1], val[2], val[3])
                if polygon.is_valid:
                    return polygon
        except Exception:
            raise ValueError('cannot convert {} to a valid'
                             ' Polygon'.format(value))

        raise ValueError('cannot convert {} to a valid'
                         ' Polygon'.format(value))

    @classmethod
    def format(cls, value: Polygon) -> str:
        return value.wkt


TimeRange = Tuple[datetime, datetime]


class TimeRangeLike(Like[TimeRange]):
    """
    Type class for temporal selection objects

    Accepts:
        1. a tuple of start/end time datetime strings ('YYYY-MM-DD', 'YYYY-MM-DD')
        2. a string of datetime string start/end dates 'YYYY-MM-DD,YYYY-MM-DD'
        3. a tuple of start/end datetime datetime objects
        4. a tuple of start/end datetime date objects

    Converts to a tuple of datetime objects
    """
    TYPE = Union[Tuple[str, str], TimeRange, Tuple[date, date], str]

    @classmethod
    def convert(cls, value: Any) -> TimeRange:
        try:
            _range = None
            if isinstance(value, tuple):
                _range = to_datetime_range(value[0], value[1])
            elif isinstance(value, str):
                val = [x.strip() for x in value.split(',')]
                _range = to_datetime_range(val[0], val[1])
            if _range:
                # Check if start date is before end date
                if _range[0] < _range[1]:
                    return _range
        except Exception:
            raise ValueError('cannot convert {} to a'
                             ' valid {}'.format(value, cls.name()))

        raise ValueError('cannot convert {} to a valid'
                         ' {}'.format(value, cls.name()))

    @classmethod
    def format(cls, value: TimeRange) -> str:
        return '{} {}'.format(value[0].isoformat(), value[1].isoformat())
