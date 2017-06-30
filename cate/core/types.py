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

import io
import ast
from abc import ABCMeta, abstractmethod
from datetime import datetime, date
from typing import Any, Generic, TypeVar, List, Union, Tuple, Optional

from shapely import wkt
from shapely.geometry import Point, Polygon, box
from shapely.geometry.base import BaseGeometry

from cate.util.misc import to_list, to_datetime_range, to_datetime
from cate.util.safe import safe_eval

T = TypeVar('T')


class Like(Generic[T], metaclass=ABCMeta):
    """
    Base class for complex types which can convert a value of varying source types into a target type *T*.
    The varying source types are therefore *like* the target type *T*.

    Subclasses shall adhere to the rule that :py:meth:`convert` shall always be able to convert from
    the ``str`` values returned from :py:meth:`format`.
    """

    #: A type that represents the varying source types. This is usually a ``typing.Union`` instance which
    #: combines the varying source types. The ``str`` type shall always be among them so that textual value
    #: representations are supported.
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

    @classmethod
    @abstractmethod
    def convert(cls, value: Any) -> Optional[T]:
        """
        Convert the given source value (of type ``Like.TYPE``) into an optional instance of type *T*.

        The general contract prescribes that values of type ``str`` shall always be allowed. In particular,
        the ``str`` values returned by the :py:meth:`format` method should always be a valid *value*.

        @:raises ValueError if the conversion fails.
        """
        pass

    @classmethod
    def format(cls, value: Optional[T]) -> str:
        """
        Convert the given optional source value of type *T* into a string.

        The general contract prescribes that the value returned shall be a valid input to :py:meth:`convert`.

        @:raises ValueError if the conversion fails.
        """
        return str(value)

    @classmethod
    def from_json(cls, value: Any) -> Optional[T]:
        """
        Deserialize the given JSON value into a value of target type *T*.

        :param value: a JSON value
        :return: a optional value of target type *T*
        """
        return cls.convert(value)

    @classmethod
    def to_json(cls, value: Optional[T]) -> Any:
        """
        Serialize the given value of type *T* into a JSON value.

        :param value: an optional value of target type *T*
        :return: a JSON value
        """
        return cls.format(value)

    @classmethod
    def assert_value_ok(cls, cond: bool, value: Any):
        if not cond:
            text_value = str(value)
            if len(text_value) > 37:
                text_value = text_value[0:38] + '...'
            raise ValueError('cannot convert value <%s> to %s' % (text_value, cls.name()))


VarNames = List[str]


class Arbitrary(Like[Any]):
    """
    Represents an arbitrary Python value.
    """
    TYPE = Any

    @classmethod
    def convert(cls, value: Any) -> Any:
        """
        Return **value**.
        """
        return value

    @classmethod
    def format(cls, value: Any) -> str:
        if value is None:
            return ''
        return str(value)


class Literal(Like[Any]):
    """
    Represents an arbitrary Python literal.
    """
    TYPE = str

    @classmethod
    def convert(cls, value: Any) -> Any:
        """
        If **value** is a string treat it as a Python literal and return its evaluation result,
        otherwise return **value**.
        """
        if value == '':
            return None
        if isinstance(value, str):
            try:
                return ast.literal_eval(value)
            except (SyntaxError, ValueError):
                cls.assert_value_ok(False, value)
        return value

    @classmethod
    def format(cls, value: Any) -> str:
        if value is None:
            return ''
        return repr(value)


class VarNamesLike(Like[VarNames]):
    """
    Type class for Variable selection objects

    Accepts:
        1. a string 'pattern1, pattern2, pattern3'
        2. a list ['pattern1', 'pattern2', 'pattern3']

    Converts to a list of strings
    """
    TYPE = Union[VarNames, str]

    @classmethod
    def convert(cls, value: Any) -> Optional[VarNames]:
        """
        Convert the given value to a list of variable name patterns.
        """
        # Can be optional
        if value is None:
            return None

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

    @classmethod
    def format(cls, value: Optional[VarNames]) -> str:
        if not value:
            return ''
        if isinstance(value, str):
            return value
        if len(value) == 1:
            return value[0]
        return ', '.join(value)


class VarName(Like[str]):
    """
    Type class for a single Variable selection object.

    Accepts:
        1. a string

    Converts to a string
    """
    TYPE = str

    @classmethod
    def convert(cls, value: Any) -> Optional[str]:
        """
        Convert the given value to a variable name
        """
        # Can be optional
        if value is None:
            return None

        cls.assert_value_ok(isinstance(value, str), value)

        return value


class FileLike(Like[dict]):
    """
    Type class for file-like objects

    Accepts:
        1. a string
        2. an io.IOBase object

    Does not convert at all.
    """

    TYPE = Union[str, io.IOBase]

    @classmethod
    def convert(cls, value: Any) -> Optional[Union[str, io.IOBase]]:
        if not value:
            return None
        if not isinstance(value, str) and not isinstance(value, io.IOBase):
            raise ValueError('File must be a path or a file handler')
        return value

    @classmethod
    def format(cls, value: Optional[Union[str, io.IOBase]]) -> str:
        return "{}".format(value) if isinstance(value, str) else ''


class DictLike(Like[dict]):
    """
    Type class for dictionary objects

    Accepts:
        1. a dictionary string
        2. a dict object

    Converts to a dict object
    """

    TYPE = Union[str, dict]

    @classmethod
    def convert(cls, value: Any) -> Optional[dict]:
        if value is None:
            return None

        if isinstance(value, dict):
            return value

        # noinspection PyBroadException
        try:
            if isinstance(value, str):
                if value.strip() == '':
                    return None
                return safe_eval('dict(%s)' % value, {})
            raise ValueError()
        except Exception:
            cls.assert_value_ok(False, value)

    @classmethod
    def format(cls, value: Optional[dict]) -> str:
        return ', '.join(['%s=%s' % (k, repr(v)) for k, v in value.items()]) if value else ''


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
    def convert(cls, value: Any) -> Optional[Point]:
        if value is None:
            return None

        # noinspection PyBroadException
        try:
            if isinstance(value, Point):
                return value
            elif isinstance(value, str):
                value = value.strip()
                if value == '':
                    return None
                pair = value.split(',')
                return Point(float(pair[0]), float(pair[1]))
            return Point(value[0], value[1])
        except Exception:
            cls.assert_value_ok(False, value)

    @classmethod
    def format(cls, value: Optional[Point]) -> str:
        return "%s, %s" % (value.x, value.y) if value else ''


class PolygonLike(Like[Polygon]):
    """
    Type class for geometric Polygon objects

    Accepts:
        1. a ``shapely.geometry.Polygon`` object
        2. a string "min_lon, min_lat, max_lon, max_lat"
        3. a WKT string "POLYGON ((RING))" or "POLYGON ((OUTER-RING), (INNER-RING), ...)"
        4. a list of coordinates [(lon, lat), (lon, lat), (lon, lat)]
        5. a list or tuple [min_lon, min_lat, max_lon, max_lat]

    Converts to a valid Shapely Polygon.
    """
    TYPE = Union[Polygon, List[Tuple[float, float]],
                 str, Tuple[float, float, float, float]]

    @classmethod
    def convert(cls, value: Any) -> Optional[Polygon]:
        if value is None:
            return None

        # noinspection PyBroadException
        try:
            if isinstance(value, Polygon):
                if value.is_valid:
                    return value
            elif isinstance(value, str):
                value = value.strip()
                if value == '':
                    return None
                if value[:7].lower() == 'polygon':
                    polygon = wkt.loads(value)
                else:
                    val = [float(x) for x in value.split(',')]
                    polygon = box(val[0], val[1], val[2], val[3])
                if polygon.is_valid:
                    return polygon
            else:
                # noinspection PyBroadException
                try:
                    polygon = Polygon(value)
                    if polygon.is_valid:
                        return polygon
                except Exception:
                    return box(float(value[0]), float(value[1]),
                               float(value[2]), float(value[3]))
        except Exception:
            pass

        cls.assert_value_ok(False, value)

    @classmethod
    def format(cls, value: Optional[Polygon]) -> str:
        return value.wkt if value else ''


class GeometryLike(Like[BaseGeometry]):
    """
    Type class for arbitrary geometry objects

    Accepts:
        1. any Shapely geometry (of type ``shapely.geometry.base.BaseGeometry``);
        2. a string 'lon, lat' (a point), or 'min_lon, min_lat, max_lon, max_lat' (a box);
        3. a Geometry WKT string starting with 'POINT', 'POLYGON', etc;
        4. a coordinate tuple (lon, lat), a list of coordinates [(lon, lat), (lon, lat), ...], or
           a list of lists of coordinates [[(lon, lat), (lon, lat), ...], [(lon, lat), (lon, lat), ...], ...].

    Converts to a valid shapely Polygon.
    """
    TYPE = Union[BaseGeometry, str, Tuple[float, float], List[Tuple[float, float]], List[List[Tuple[float, float]]]]

    @classmethod
    def convert(cls, value: Any) -> Optional[BaseGeometry]:
        if value is None:
            return None

        # TODO (forman): Fully implement me! We here utilise PointLike and PolygonLike for time being.
        try:
            return PolygonLike.convert(value)
        except ValueError:
            try:
                return PointLike.convert(value)
            except ValueError:
                pass
        cls.assert_value_ok(False, value)

    @classmethod
    def accepts(cls, value: Any) -> bool:
        # TODO (forman): Fully implement me! We here utilise PointLike and PolygonLike for time being.
        return PolygonLike.accepts(value) or PointLike.accepts(value)

    @classmethod
    def format(cls, value: BaseGeometry) -> str:
        return value.wkt if value else ''


class TimeLike(Like[datetime]):
    """
    Type class for a time-like object.

    Accepts:
        2. a string with format 'YYYY-MM-DD'
        3. a datetime object
        4. a date object

    """
    TYPE = Union[str, datetime, date]

    @classmethod
    def convert(cls, value: Any) -> Optional[datetime]:
        # Can be optional
        if value is None or isinstance(value, datetime):
            return value

        if isinstance(value, str) and value.strip() == '':
            return None

        if isinstance(value, date) or isinstance(value, str):
            # noinspection PyBroadException
            try:
                return to_datetime(value)
            except Exception:
                pass

        cls.assert_value_ok(False, value)

    @classmethod
    def format(cls, value: Optional[datetime]) -> str:
        return _to_isoformat(value)


_ZERO_ISO_TIME_PART = 'T00:00:00'


def _to_isoformat(value: datetime) -> str:
    if not value:
        return ''
    text = value.isoformat()
    if text.endswith(_ZERO_ISO_TIME_PART):
        return text[0:-len(_ZERO_ISO_TIME_PART)]
    return text


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
    def convert(cls, value: Any) -> Optional[TimeRange]:
        # Can be optional
        if value is None or value == '':
            return None

        # noinspection PyBroadException
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
            pass

        cls.assert_value_ok(False, value)

    @classmethod
    def format(cls, value: Optional[TimeRange]) -> str:
        if not value:
            return ''
        return '{}, {}'.format(_to_isoformat(value[0]), _to_isoformat(value[1]))


import xarray as xr
import pandas as pd


class DatasetLike(Like[xr.Dataset]):
    TYPE = Union[xr.Dataset, pd.DataFrame, None]

    @classmethod
    def convert(cls, value: Any) -> Optional[xr.Dataset]:
        # Can be optional
        if value is None:
            return None
        if isinstance(value, xr.Dataset):
            return value
        if isinstance(value, pd.DataFrame):
            return xr.Dataset.from_dataframe(value)
        raise ValueError('Value must be an xr.Dataset or pd.DataFrame')

    @classmethod
    def format(cls, value: Optional[xr.Dataset]) -> str:
        if value is None:
            return ''
        raise ValueError('Values of type DatasetLike cannot be converted to text')


class DataFrameLike(Like[pd.DataFrame]):
    TYPE = Union[pd.DataFrame, xr.Dataset, None]

    @classmethod
    def convert(cls, value: Any) -> Optional[pd.DataFrame]:
        # Can be optional
        if value is None:
            return None
        if isinstance(value, pd.DataFrame):
            return value
        if isinstance(value, xr.Dataset):
            return value.to_dataframe()
        raise ValueError('Value must be a pd.DataFrame or xr.Dataset')

    @classmethod
    def format(cls, value: Optional[pd.DataFrame]) -> str:
        if value is None:
            return ''
        raise ValueError('Values of type DataFrameLike cannot be converted to text')
