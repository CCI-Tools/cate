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

import ast
import io
from abc import ABCMeta, abstractmethod
from datetime import datetime, date
from typing import Generic, TypeVar, Union, Optional, Any, Tuple, List

import geopandas
import pandas
import shapely
import shapely.geometry
import shapely.geometry.base
import shapely.wkt
import xarray
from shapely.errors import ShapelyError

from ..util.misc import to_list, to_datetime_range, to_datetime
from ..util.safe import safe_eval

__author__ = "Janis Gailis (S[&]T Norway), " \
             "Norman Fomferra (Brockmann Consult GmbH), " \
             "Marco Zühlke (Brockmann Consult GmbH)"

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


class HTML(Like[str]):
    """
    Represents HTML string
    """
    TYPE = str

    @classmethod
    def convert(cls, value: str) -> str:
        """
        Return **value**
        """
        return value

    @classmethod
    def format(cls, value: str) -> str:
        if value is None:
            return ''
        return value


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

        return value.copy()

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


class BaseGeometryLike(Generic[T], Like[T], metaclass=ABCMeta):

    @classmethod
    def format(cls, value: shapely.geometry.base.BaseGeometry) -> str:
        return value.wkt if value else ''

    @classmethod
    def to_geometry(cls, value: Any, geom_type: type = shapely.geometry.base.BaseGeometry) \
            -> Optional[shapely.geometry.base.BaseGeometry]:
        if value is None:
            return None

        if isinstance(value, geom_type):
            # Compatible!
            # noinspection PyTypeChecker
            return value

        if isinstance(value, shapely.geometry.base.BaseGeometry):
            # Incompatible!
            raise ValueError(cls.errmsg("passed geometry type is incompatible"))

        # Try converting to compatible type
        if isinstance(value, str):
            value = value.strip()
            if value == '':
                # Empty strings are same as None
                return None

            try:
                try:
                    coordinates = [float(v) for v in value.split(',', maxsplit=3)]
                    if len(coordinates) == 2 and cls._is_compatible_type(geom_type, shapely.geometry.Point):
                        # Point "x, y"?
                        x, y = coordinates
                        return shapely.geometry.Point(x, y)
                    elif len(coordinates) == 4 and cls._is_compatible_type(geom_type, shapely.geometry.Polygon):
                        # Polygon box "x1, y1, x2, y2"?
                        x1, y1, x2, y2 = coordinates
                        return shapely.geometry.box(x1, y1, x2, y2)
                except (ValueError, TypeError):
                    # Geometry WKT?
                    return shapely.wkt.loads(value)
            except (ShapelyError, ValueError, TypeError) as e:
                raise ValueError(cls.errmsg('invalid geometry WKT format')) from e

            raise ValueError(cls.errmsg("unrecognized text format"))

        if cls._is_compatible_type(geom_type, shapely.geometry.Point):
            try:
                # Coordinates list that forms a polygon?
                return shapely.geometry.Point(value)
            except (ShapelyError, ValueError, TypeError):
                pass

        if cls._is_compatible_type(geom_type, shapely.geometry.Polygon):
            try:
                # Coordinates list that forms a polygon?
                return shapely.geometry.Polygon(value)
            except (ShapelyError, ValueError, TypeError):
                # Polygon box x1, y1, x2, y2?
                try:
                    x1, y1, x2, y2 = value
                    return shapely.geometry.box(x1, y1, x2, y2)
                except (ShapelyError, ValueError, TypeError) as e:
                    raise ValueError(cls.errmsg(e)) from e

        raise ValueError(cls.errmsg())

    @classmethod
    def errmsg(cls, detail_msg=None) -> str:
        if detail_msg:
            return 'cannot convert value to %s: %s' % (cls.name(), detail_msg)
        else:
            return 'cannot convert value to %s' % cls.name()

    @classmethod
    def _is_compatible_type(cls, geom_type, required_type):
        return geom_type == shapely.geometry.base.BaseGeometry or issubclass(geom_type, required_type)


class GeometryLike(BaseGeometryLike[shapely.geometry.base.BaseGeometry]):
    """
    Type class for arbitrary geometry objects

    Accepts:
        1. any Shapely geometry (of type ``shapely.geometry.base.BaseGeometry``);
        2. a string 'lon, lat' (a point), or 'min_lon, min_lat, max_lon, max_lat' (a shapely.geometry.box);
        3. a Geometry WKT string starting with 'POINT', 'POLYGON', etc;
        4. a coordinate tuple (lon, lat), a list of coordinates [(lon, lat), (lon, lat), ...], or
           a list of lists of coordinates [[(lon, lat), (lon, lat), ...], [(lon, lat), (lon, lat), ...], ...].

    Converts to a valid shapely geometry.
    """
    TYPE = Union[shapely.geometry.base.BaseGeometry,
                 str,
                 Tuple[float, float],
                 List[Tuple[float, float]],
                 List[List[Tuple[float, float]]]]

    @classmethod
    def convert(cls, value: Any) -> Optional[shapely.geometry.base.BaseGeometry]:
        geometry = cls.to_geometry(value, shapely.geometry.base.BaseGeometry)

        if isinstance(geometry, shapely.geometry.Polygon) and geometry.is_valid:
            # Heal polygon, see #506 and Shapely User Manual
            geometry = geometry.buffer(0)

        return geometry


class PointLike(BaseGeometryLike[shapely.geometry.Point]):
    """
    Type class for geometric shapely.geometry.Point objects

    Accepts:
        1. a Shapely shapely.geometry.Point
        2. a string 'lon,lat'
        4. a WKT string "POINT (lon lat)"
        5. a tuple (lon, lat)

    Converts to a Shapely shapely.geometry.Point object.
    """
    TYPE = Union[shapely.geometry.Point, str, Tuple[float, float]]

    @classmethod
    def convert(cls, value: Any) -> Optional[shapely.geometry.Point]:
        return cls.to_geometry(value, shapely.geometry.Point)

    @classmethod
    def format(cls, value: Optional[shapely.geometry.Point]) -> str:
        return "%s, %s" % (value.x, value.y) if value else ''


class PolygonLike(GeometryLike, Like[shapely.geometry.Polygon]):
    """
    Type class for geometric shapely.geometry.Polygon objects

    Accepts:
        1. a ``shapely.geometry.shapely.geometry.Polygon`` object
        2. a string "min_lon, min_lat, max_lon, max_lat"
        3. a WKT string "POLYGON ((RING))" or "POLYGON ((OUTER-RING), (INNER-RING), ...)"
        4. a list of coordinates [(lon, lat), (lon, lat), (lon, lat)]
        5. a list or tuple [min_lon, min_lat, max_lon, max_lat]

    Converts to a valid Shapely shapely.geometry.Polygon.
    """
    TYPE = Union[shapely.geometry.Polygon, List[Tuple[float, float]],
                 str, Tuple[float, float, float, float]]

    @classmethod
    def convert(cls, value: Any) -> Optional[shapely.geometry.Polygon]:
        polygon = cls.to_geometry(value, shapely.geometry.Polygon)
        if polygon is None:
            return None

        assert isinstance(polygon, shapely.geometry.Polygon)

        if not polygon.is_valid:
            # Heal polygon, see #506 and Shapely User Manual
            polygon = polygon.buffer(0)
            if not polygon.is_valid:
                raise ValueError(cls.errmsg("polygon is invalid"))

        if polygon.is_empty:
            raise ValueError(cls.errmsg("polygon is empty"))

        return polygon

    @classmethod
    def format(cls, value: Optional[shapely.geometry.Polygon]) -> str:
        return value.wkt if value else ''


class TimeLike(Like[datetime]):
    """
    Type class for a time-like object.

    Accepts:
        2. a string with format 'YYYY-MM-DD';
        3. a datetime object;
        4. a date object.

    Converts to datetime object.
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
        1. a tuple of start/end time datetime strings ('YYYY-MM-DD', 'YYYY-MM-DD');
        2. a string of datetime string start/end dates 'YYYY-MM-DD,YYYY-MM-DD';
        3. a tuple of start/end datetime datetime objects;
        4. a tuple of start/end datetime date objects.

    Converts to a tuple of datetime objects.
    """
    TYPE = Union[Tuple[str, str], TimeRange, Tuple[date, date], str]

    @classmethod
    def convert(cls, value: Any) -> Optional[TimeRange]:
        # Can be optional
        if value is None or value == '':
            return None

        if isinstance(value, str):
            value = [x.strip() for x in value.split(',')]

        try:
            t1, t2 = value[0], value[1]
        except IndexError:
            cls.assert_value_ok(False, value)
            return

        time_range = to_datetime_range(t1, t2)

        # Check if start date is before end date
        if not time_range or time_range[0] < time_range[1]:
            return time_range

        cls.assert_value_ok(False, value)

    @classmethod
    def format(cls, value: Optional[TimeRange]) -> str:
        if not value:
            return ''
        return '{}, {}'.format(_to_isoformat(value[0]), _to_isoformat(value[1]))


class DatasetLike(Like[xarray.Dataset]):
    """
    Accepts xarray.Dataset, pandas.DataFrame and converts to xarray.Dataset.
    """

    TYPE = Union[xarray.Dataset, pandas.DataFrame, None]

    @classmethod
    def convert(cls, value: Any) -> Optional[xarray.Dataset]:
        # Can be optional
        from cate.core.opimpl import adjust_temporal_attrs_impl

        if value is None:
            return None
        if isinstance(value, xarray.Dataset):
            return value
        if isinstance(value, pandas.DataFrame):
            return adjust_temporal_attrs_impl(xarray.Dataset.from_dataframe(value))
        raise ValueError('Value must be an xarray.Dataset or pandas.DataFrame')

    @classmethod
    def format(cls, value: Optional[xarray.Dataset]) -> str:
        if value is None:
            return ''
        raise ValueError('Values of type DatasetLike cannot be converted to text')


class DataFrameLike(Like[pandas.DataFrame]):
    """
    Accepts pandas.DataFrame, xarray.Dataset and converts to pandas.DataFrame.
    """

    TYPE = Union[pandas.DataFrame, xarray.Dataset, None]

    @classmethod
    def convert(cls, value: Any) -> Optional[pandas.DataFrame]:
        # Can be optional
        if value is None:
            return None
        if isinstance(value, pandas.DataFrame):
            return value
        if isinstance(value, xarray.Dataset):
            return value.to_dataframe()
        raise ValueError('Value must be a pandas.DataFrame or xarray.Dataset')

    @classmethod
    def format(cls, value: Optional[pandas.DataFrame]) -> str:
        if value is None:
            return ''
        raise ValueError('Values of type DataFrameLike cannot be converted to text')


class GeoDataFrame:
    """
    Proxy for a ``geopandas.GeoDataFrame`` that holds an iterable of features or a feature collection
    for fastest possible streaming of GeoJSON features to be consumed by Cate Desktop's 3D globes.
    """

    _OWN_PROPERTY_SET = {
        "_features",
        "features",
        "_lazy_data_frame",
        "lazy_data_frame",
        "close",
    }

    @classmethod
    def from_features(cls, features):
        """
        Create GeoDataFrame from an iterable of features or a feature collection.

        :param features: iterable of features or a feature collection, e.g. an open ``fiona.Collection`` instance.
        :return: An instance of a ``GeoDataFrame`` proxy.
        """
        return cls(features)

    def __init__(self, features):
        if features is None:
            raise ValueError('features must not be None')
        self._features = features
        self._lazy_data_frame = None

    @property
    def features(self):
        return self._features

    @property
    def lazy_data_frame(self):
        features = self._features
        if features is not None and self._lazy_data_frame is None:
            self._lazy_data_frame = geopandas.GeoDataFrame.from_features(features, crs=features.crs)
        return self._lazy_data_frame

    def close(self):
        """
        In Cate, closable resources are closed when removed from the resources cache.
        Therefore we provide a close method here, although geopandas.GeoDataFrame doesn't have one.
        """
        try:
            self._features.close()
        except AttributeError:
            pass
        self._features = None
        self._lazy_data_frame = None

    def __setattr__(self, key, value):
        # print('__setattr__({}, {})'.format(repr(key), repr(value)))
        if key in GeoDataFrame._OWN_PROPERTY_SET:
            object.__setattr__(self, key, value)
        else:
            self.lazy_data_frame.__setattr__(key, value)

    def __getattribute__(self, item):
        # print('__getattribute__({})'.format(repr(item)))
        if item in GeoDataFrame._OWN_PROPERTY_SET:
            return object.__getattribute__(self, item)
        else:
            return getattr(self.lazy_data_frame, item)

    def __getattr__(self, item):
        # print('__getattr__({})'.format(repr(item)))
        if item in GeoDataFrame._OWN_PROPERTY_SET:
            return object.__getattribute__(self, item)
        else:
            return getattr(self.lazy_data_frame, item)
        # raise RuntimeError("%s is not an owned property" % item)

    def __getitem__(self, item):
        return self.lazy_data_frame.__getitem__(item)

    def __setitem__(self, key, value):
        return self.lazy_data_frame.__setitem__(key, value)

    def __str__(self):
        return str(self.lazy_data_frame)

    def __repr__(self):
        return repr(self.lazy_data_frame)

    def __len__(self):
        # perf: using self._features here to avoid instantiation of GeoDataFrame._lazy_data_frame
        return len(self._features)

    # Add other __x__() methods here to make GeoDataFrame compatible with geopandas.GeoDataFrame
