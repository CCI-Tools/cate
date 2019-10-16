from collections import namedtuple
from datetime import datetime, date
from io import StringIO
from typing import Union, Tuple
from unittest import TestCase

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely.wkt
import xarray as xr
from shapely.geometry import Point, Polygon

from cate.core.op import op_input, OpRegistry
from cate.core.types import Like, VarNamesLike, VarName, PointLike, PolygonLike, TimeRangeLike, GeometryLike, \
    DictLike, TimeLike, Arbitrary, Literal, DatasetLike, DataFrameLike, FileLike, GeoDataFrame, HTMLLike, HTML, \
    ValidationError, DimName, DimNamesLike
from cate.util.misc import object_to_qualified_name, OrderedDict

# 'ExamplePoint' is an example type which may come from Cate API or other required API.
ExamplePoint = namedtuple('ExamplePoint', ['x', 'y'])


# 'ExampleType' represents a type to be used for values that should actually be a 'ExamplePoint' but
# also have a representation as a `str` such as "2.1,4.3", ar a tuple of two floats (2.1,4.3).
# The "typing type" for this is given by ExampleType.TYPE.

class ExampleType(Like[ExamplePoint]):

    TYPE = Union[ExamplePoint, Tuple[float, float], str]

    @classmethod
    def convert(cls, value, default=None) -> ExamplePoint:
        try:
            if isinstance(value, ExamplePoint):
                return value
            if isinstance(value, str):
                pair = value.split(',')
                return ExamplePoint(float(pair[0]), float(pair[1]))
            return ExamplePoint(value[0], value[1])
        except Exception:
            raise ValidationError('Cannot convert value <%s> to %s.' % (repr(value), cls.name()))

    @classmethod
    def format(cls, value: ExamplePoint) -> str:
        return "%s, %s" % (value.x, value.y)


# TestType = NewType('TestType', _TestType)


# 'scale_point' is an example operation that makes use of the TestType type for argument point_like

_OP_REGISTRY = OpRegistry()


@op_input("point_like", data_type=ExampleType, registry=_OP_REGISTRY)
def scale_point(point_like: ExampleType.TYPE, factor: float) -> ExamplePoint:
    point = ExampleType.convert(point_like)
    return ExamplePoint(factor * point.x, factor * point.y)


class ExampleTypeTest(TestCase):
    def test_use(self):
        self.assertEqual(scale_point("2.4, 4.8", 0.5), ExamplePoint(1.2, 2.4))
        self.assertEqual(scale_point((2.4, 4.8), 0.5), ExamplePoint(1.2, 2.4))
        self.assertEqual(scale_point(ExamplePoint(2.4, 4.8), 0.5), ExamplePoint(1.2, 2.4))

    def test_abuse(self):
        with self.assertRaises(ValidationError) as cm:
            scale_point("A, 4.8", 0.5)
        self.assertEqual(str(cm.exception),
                         "Input 'point_like' for operation 'test.core.test_types.scale_point': "
                         "Cannot convert value <'A, 4.8'> to ExampleType.")

        with self.assertRaises(ValidationError) as cm:
            scale_point(25.1, 0.5)
        self.assertEqual(str(cm.exception),
                         "Input 'point_like' for operation 'test.core.test_types.scale_point': "
                         "Cannot convert value <25.1> to ExampleType.")

    def test_registered_op(self):
        registered_op = _OP_REGISTRY.get_op(object_to_qualified_name(scale_point))
        point = registered_op(point_like="2.4, 4.8", factor=0.5)
        self.assertEqual(point, ExamplePoint(1.2, 2.4))

    def test_name(self):
        self.assertEqual(ExampleType.name(), "ExampleType")

    def test_accepts(self):
        self.assertTrue(ExampleType.accepts("2.4, 4.8"))
        self.assertTrue(ExampleType.accepts((2.4, 4.8)))
        self.assertTrue(ExampleType.accepts([2.4, 4.8]))
        self.assertTrue(ExampleType.accepts(ExamplePoint(2.4, 4.8)))
        self.assertFalse(ExampleType.accepts("A, 4.8"))
        self.assertFalse(ExampleType.accepts(25.1))

    def test_format(self):
        self.assertEqual(ExampleType.format(ExamplePoint(2.4, 4.8)), "2.4, 4.8")


class HTMLLikeTest(TestCase):
    """
    Test the HTMLLike type
    """

    def test_accepts(self):
        self.assertTrue(HTMLLike.accepts('abc'))
        self.assertTrue(HTMLLike.accepts(HTML('abc')))
        self.assertTrue(HTMLLike.accepts(42))

    def test_convert(self):
        actual = HTMLLike.convert('abc')
        self.assertIsInstance(actual, HTML)
        self.assertEqual(actual, 'abc')

    def test_format(self):
        actual = VarNamesLike.format(HTML('abc'))
        self.assertIsInstance(actual, str)
        self.assertEqual(actual, 'abc')


class VarNamesLikeTest(TestCase):
    """
    Test the VarNamesLike type
    """

    def test_accepts(self):
        self.assertTrue(VarNamesLike.accepts('aa'))
        self.assertTrue(VarNamesLike.accepts('aa,bb,cc'))
        self.assertTrue(VarNamesLike.accepts(['aa', 'bb', 'cc']))
        self.assertFalse(VarNamesLike.accepts(1.0))
        self.assertFalse(VarNamesLike.accepts([1, 2, 4]))
        self.assertFalse(VarNamesLike.accepts(['aa', 2, 'bb']))

    def test_convert(self):
        expected = ['aa', 'b*', 'cc']
        actual = VarNamesLike.convert('aa,b*,cc')
        self.assertEqual(actual, expected)

        with self.assertRaises(ValidationError) as err:
            VarNamesLike.convert(['aa', 1, 'bb'])
        self.assertEqual(str(err.exception), 'List of variables names expected.')
        self.assertEqual(None, VarNamesLike.convert(None))

    def test_format(self):
        self.assertEqual(VarNamesLike.format(['aa', 'bb', 'cc']), "aa, bb, cc")
        self.assertEqual(VarNamesLike.format(['aa']), "aa")
        self.assertEqual(VarNamesLike.format([]), "")
        self.assertEqual(VarNamesLike.format(None), "")


class VarNameTest(TestCase):
    """
    Test the VarName type
    """

    def test_accepts(self):
        self.assertTrue(VarName.accepts('aa'))
        self.assertFalse(VarName.accepts(['aa', 'bb', 'cc']))
        self.assertFalse(VarName.accepts(1.0))

    def test_convert(self):
        expected = 'aa'
        actual = VarName.convert('aa')
        self.assertEqual(actual, expected)

        with self.assertRaises(ValidationError) as err:
            VarName.convert(['aa', 'bb', 'cc'])
        self.assertEqual(str(err.exception), 'Variable name expected.')
        self.assertEqual(None, VarName.convert(None))

    def test_format(self):
        self.assertEqual('aa', VarName.format('aa'))


class DimNamesLikeTest(TestCase):
    """
    Test the DimNamesLike type
    """

    def test_accepts(self):
        self.assertTrue(DimNamesLike.accepts('aa'))
        self.assertTrue(DimNamesLike.accepts('aa,bb,cc'))
        self.assertTrue(DimNamesLike.accepts(['aa', 'bb', 'cc']))
        self.assertFalse(DimNamesLike.accepts(1.0))
        self.assertFalse(DimNamesLike.accepts([1, 2, 4]))
        self.assertFalse(DimNamesLike.accepts(['aa', 2, 'bb']))

    def test_convert(self):
        expected = ['aa', 'b*', 'cc']
        actual = DimNamesLike.convert('aa,b*,cc')
        self.assertEqual(actual, expected)

        with self.assertRaises(ValidationError) as err:
            DimNamesLike.convert(['aa', 1, 'bb'])
        self.assertEqual(str(err.exception), 'List of dimension names expected.')
        self.assertEqual(None, DimNamesLike.convert(None))

    def test_format(self):
        self.assertEqual(DimNamesLike.format(['aa', 'bb', 'cc']), "aa, bb, cc")
        self.assertEqual(DimNamesLike.format(['aa']), "aa")
        self.assertEqual(DimNamesLike.format([]), "")
        self.assertEqual(DimNamesLike.format(None), "")


class DimNameTest(TestCase):
    """
    Test the DimName type
    """

    def test_accepts(self):
        self.assertTrue(DimName.accepts('aa'))
        self.assertFalse(DimName.accepts(['aa', 'bb', 'cc']))
        self.assertFalse(DimName.accepts(1.0))

    def test_convert(self):
        expected = 'aa'
        actual = DimName.convert('aa')
        self.assertEqual(actual, expected)

        with self.assertRaises(ValidationError) as err:
            DimName.convert(['aa', 'bb', 'cc'])
        self.assertEqual(str(err.exception), 'Dimension name expected.')
        self.assertEqual(None, DimName.convert(None))

    def test_format(self):
        self.assertEqual('aa', DimName.format('aa'))


class FileLikeTest(TestCase):
    """
    Test the FileLike type
    """

    def test_accepts(self):
        self.assertTrue(FileLike.accepts(None))
        self.assertTrue(FileLike.accepts(''))
        self.assertTrue(FileLike.accepts('a/b/c'))
        self.assertTrue(FileLike.accepts(StringIO()))
        self.assertFalse(FileLike.accepts(2))
        self.assertFalse(FileLike.accepts(True))

    def test_convert(self):
        self.assertEqual(FileLike.convert(None), None)
        self.assertEqual(FileLike.convert(''), None)
        self.assertEqual(FileLike.convert('a/b/c'), 'a/b/c')
        io = StringIO()
        self.assertEqual(FileLike.convert(io), io)

    def test_format(self):
        self.assertEqual(FileLike.format(None), '')
        self.assertEqual(FileLike.format('a/b/c'), 'a/b/c')
        io = StringIO()
        self.assertEqual(FileLike.format(io), '')


class DictLikeTest(TestCase):
    """
    Test the DictLike type
    """

    def test_accepts(self):
        self.assertTrue(DictLike.accepts(None))
        self.assertTrue(DictLike.accepts(''))
        self.assertTrue(DictLike.accepts('  '))
        self.assertTrue(DictLike.accepts('a=6, b=5.3, c=True, d="Hello"'))

        self.assertFalse(DictLike.accepts('{a=True}'))
        self.assertFalse(DictLike.accepts('a=true'))
        self.assertFalse(DictLike.accepts('{a, b}'))
        self.assertFalse(DictLike.accepts(['aa', 'bb', 'cc']))
        self.assertFalse(DictLike.accepts(1.0))

    def test_convert(self):
        self.assertEqual(DictLike.convert(None), None)
        self.assertEqual(DictLike.convert(''), None)
        self.assertEqual(DictLike.convert('  '), None)
        self.assertEqual(DictLike.convert('name="bibo", thres=0.5, drop=False'),
                         dict(name="bibo", thres=0.5, drop=False))

        with self.assertRaises(ValidationError) as err:
            DictLike.convert('{a=8, b}')
        self.assertEqual(str(err.exception), "Value '{a=8, b}' cannot be converted into a 'DictLike'.")

    def test_format(self):
        self.assertEqual(DictLike.format(OrderedDict([('name', 'bibo'), ('thres', 0.5), ('drop', True)])),
                         "name='bibo', thres=0.5, drop=True")

    def test_to_json(self):
        self.assertEqual(DictLike.to_json(OrderedDict([('name', 'bibo'), ('thres', 0.5), ('drop', True)])),
                         "name='bibo', thres=0.5, drop=True")

    def test_from_json(self):
        self.assertEqual(DictLike.from_json("name='bibo', thres=0.5, drop=True"),
                         dict(name='bibo', thres=0.5, drop=True))


class PointLikeTest(TestCase):
    """
    Test the PointLike type
    """

    def test_accepts(self):
        self.assertTrue(PointLike.accepts(""))
        self.assertTrue(PointLike.accepts("\t\n "))
        self.assertTrue(PointLike.accepts("2.4, 4.8\n"))
        self.assertTrue(PointLike.accepts((2.4, 4.8)))
        self.assertTrue(PointLike.accepts([2.4, 4.8]))
        self.assertTrue(PointLike.accepts(Point(2.4, 4.8)))
        self.assertTrue(PointLike.accepts(Point(2.4, 4.8).wkt))
        self.assertFalse(PointLike.accepts("A, 4.8"))
        self.assertFalse(PointLike.accepts(25.1))

    def test_convert(self):
        self.assertEqual(PointLike.convert(None), None)
        self.assertEqual(PointLike.convert(''), None)
        self.assertEqual(PointLike.convert('0.0,1.0'), Point(0.0, 1.0))
        with self.assertRaises(ValidationError) as err:
            PointLike.convert('0.0,abc')
        self.assertEqual(str(err.exception), "Value cannot be converted into a 'PointLike': "
                                             "Invalid geometry WKT format.")
        self.assertEqual(PointLike.convert('POINT(0.0 1.0)'), Point(0.0, 1.0))

    def test_format(self):
        self.assertEqual(PointLike.format(Point(2.4, 4.8)), "2.4, 4.8")


class PolygonLikeTest(TestCase):
    """
    Test the PolygonLike type
    """

    def test_accepts(self):
        self.assertTrue(PolygonLike.accepts(""))
        self.assertTrue(PolygonLike.accepts(" \t"))
        self.assertTrue(PolygonLike.accepts("0.0,0.0,1.1,1.1"))
        self.assertTrue(PolygonLike.accepts("0.0, 0.0, 1.1, 1.1"))

        coords = [(10.4, 20.2), (30.8, 20.2), (30.8, 40.8), (10.4, 40.8)]
        pol = Polygon(coords)
        self.assertTrue(PolygonLike.accepts(coords))
        self.assertTrue(PolygonLike.accepts(pol))
        self.assertTrue(PolygonLike.accepts(pol.wkt))
        self.assertTrue(PolygonLike.accepts(pol.bounds))

        self.assertFalse(PolygonLike.accepts("0.0,aaa,1.1,1.1"))
        self.assertFalse(PolygonLike.accepts("0.0, aaa, 1.1, 1.1"))
        self.assertFalse(PolygonLike.accepts([(0.0, 0.0), (0.0, 1.0), (1.0, 'aaa'), (1.0, 0.0)]))
        self.assertFalse(PolygonLike.accepts([(0.0, 0.0), (0.0, 1.0), 'Guten Morgen, Berlin!', (1.0, 0.0)]))
        self.assertFalse(PolygonLike.accepts(Polygon([(0.0, 0.0), (0.0, 0.0), (0.0, 0.0), (0.0, 0.0)])))
        self.assertFalse(PolygonLike.accepts('MULTIPOLYGON()'))
        self.assertFalse(PolygonLike.accepts('Something_in_the_month_of_May'))
        self.assertFalse(PolygonLike.accepts(1.0))

    def test_convert(self):
        self.assertEqual(PolygonLike.convert(None), None)
        self.assertEqual(PolygonLike.convert(''), None)
        coords = [(10.4, 20.2), (30.8, 20.2), (30.8, 40.8), (10.4, 40.8)]
        self.assertTrue(PolygonLike.convert(coords), Polygon(coords))
        self.assertTrue(PolygonLike.convert([10.4, 20.2, 30.8, 40.8]), Polygon(coords))

        with self.assertRaises(ValidationError) as err:
            PolygonLike.convert('aaa')
        self.assertEqual(str(err.exception),
                         "Value cannot be converted into a 'PolygonLike': "
                         "Invalid geometry WKT format.")

    def test_format(self):
        self.assertEqual(PolygonLike.format(None), '')
        coords = [(10.4, 20.2), (30.8, 20.2), (30.8, 40.8), (10.4, 40.8)]
        pol = PolygonLike.convert(coords)
        self.assertEqual(PolygonLike.format(pol), 'POLYGON ((10.4 20.2, 30.8 20.2, 30.8 40.8, 10.4 40.8, 10.4 20.2))')

    def test_json(self):
        self.assertEqual(PolygonLike.from_json("-10, -10, 10, 10"), "-10, -10, 10, 10")


class GeometryLikeTest(TestCase):

    def test_accepts(self):
        self.assertTrue(GeometryLike.accepts("10, 10"))
        self.assertTrue(GeometryLike.accepts("10, 10, 20, 20"))
        self.assertTrue(GeometryLike.accepts([(10, 10), (20, 10), (20, 20), (10, 20), (10, 10)]))
        self.assertTrue(GeometryLike.accepts(shapely.wkt.loads("POINT (10 10)")))
        self.assertTrue(GeometryLike.accepts(shapely.wkt.loads("POINT (10 10)")))
        self.assertTrue(GeometryLike.accepts(shapely.wkt.loads("MULTIPOINT "
                                                               "(10 10, 20 10, 20 20, 10 20, 10 10)")))
        self.assertTrue(GeometryLike.accepts(shapely.wkt.loads("LINESTRING "
                                                               "(10 10, 20 10, 20 20, 10 20, 10 10)")))
        self.assertTrue(GeometryLike.accepts(shapely.wkt.loads("MULTILINESTRING "
                                                               "((10 10, 20 10, 20 20, 10 20, 10 10))")))
        self.assertTrue(GeometryLike.accepts(shapely.wkt.loads("POLYGON "
                                                               "((10 10, 20 10, 20 20, 10 20, 10 10))")))
        self.assertTrue(GeometryLike.accepts(shapely.wkt.loads("MULTIPOLYGON "
                                                               "(((10 10, 20 10, 20 20, 10 20, 10 10)))")))
        self.assertTrue(GeometryLike.accepts(shapely.wkt.loads("GEOMETRYCOLLECTION "
                                                               "(POINT (10 10), "
                                                               "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10)))")))
        self.assertTrue(GeometryLike.accepts([(0.0, 0.0), (0.0, 0.0), (0.0, 0.0), (0.0, 0.0)]))

        self.assertFalse(GeometryLike.accepts("0.0,aaa,1.1,1.1"))
        self.assertFalse(GeometryLike.accepts("0.0, aaa, 1.1, 1.1"))

        # empty = Polygon([(0.0, 0.0), (0.0, 0.0), (0.0, 0.0), (0.0, 0.0)])
        self.assertFalse(GeometryLike.accepts('MULTIPOLYGON()'))
        self.assertFalse(GeometryLike.accepts('Something_in_the_month_of_May'))
        self.assertFalse(GeometryLike.accepts(1.0))

    def test_convert(self):
        self.assertEqual(GeometryLike.convert(None), None)
        self.assertEqual(GeometryLike.convert(""), None)
        coords = [(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)]
        self.assertTrue(GeometryLike.convert(coords), Polygon(coords))

        with self.assertRaises(ValidationError) as err:
            GeometryLike.convert('aaa')
        self.assertEqual(str(err.exception), "Value cannot be converted into a 'GeometryLike': "
                                             "Invalid geometry WKT format.")

    def test_format(self):
        pol = GeometryLike.convert([(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)])
        self.assertTrue(GeometryLike.format(pol), 'POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))')


class TimeLikeTest(TestCase):
    """
    Test the TimeLike type
    """

    def test_accepts(self):
        self.assertTrue(TimeLike.accepts(None))
        self.assertTrue(TimeLike.accepts(''))
        self.assertTrue(TimeLike.accepts(' '))
        self.assertTrue(TimeLike.accepts('2001-01-01'))
        self.assertTrue(TimeLike.accepts(datetime(2001, 1, 1)))
        self.assertTrue(TimeLike.accepts(date(2001, 1, 1)))

        self.assertFalse(TimeLike.accepts('4.3'))
        self.assertFalse(TimeLike.accepts('2001-01-01,2001-02-01,'))
        self.assertFalse(TimeLike.accepts([datetime(2001, 1, 1)]))

    def test_convert(self):
        self.assertEqual(TimeLike.convert('2017-04-19'), datetime(2017, 4, 19))
        self.assertEqual(TimeLike.convert(datetime(2017, 4, 19)), datetime(2017, 4, 19))
        self.assertEqual(TimeLike.convert(date(2017, 4, 19)), datetime(2017, 4, 19, 12))
        self.assertEqual(TimeLike.convert('  '), None)
        self.assertEqual(TimeLike.convert(None), None)

    def test_format(self):
        self.assertEqual(TimeLike.format(None), '')
        self.assertEqual(TimeLike.format(datetime(2017, 4, 19)), '2017-04-19')

    def test_json(self):
        self.assertEqual(TimeLike.to_json(datetime(2017, 4, 19)), '2017-04-19')
        self.assertEqual(TimeLike.from_json('2017-04-19'), datetime(2017, 4, 19))


class TimeRangeLikeTest(TestCase):
    """
    Test the TimeRangeLike type
    """

    def test_accepts(self):
        self.assertTrue(TimeRangeLike.accepts(('2001-01-01', '2002-02-01')))
        self.assertTrue(TimeRangeLike.accepts((datetime(2001, 1, 1), datetime(2002, 2, 1))))
        self.assertTrue(TimeRangeLike.accepts((date(2001, 1, 1), date(2002, 1, 1))))
        self.assertTrue(TimeRangeLike.accepts('2001-01-01,2002-02-01'))
        self.assertTrue(TimeRangeLike.accepts('2001-01-01, 2002-02-01'))

        self.assertFalse(TimeRangeLike.accepts('2001-01-01'))
        self.assertFalse(TimeRangeLike.accepts([datetime(2001, 1, 1)]))
        self.assertFalse(TimeRangeLike.accepts('2002-01-01, 2001-01-01'))

    def test_convert(self):
        self.assertEqual(TimeRangeLike.convert(None), None)
        self.assertEqual(TimeRangeLike.convert((None, None)), None)
        self.assertEqual(TimeRangeLike.convert([None, None]), None)
        self.assertEqual(TimeRangeLike.convert(''), None)
        self.assertEqual(TimeRangeLike.convert((datetime(2001, 1, 1), datetime(2002, 2, 1))),
                         (datetime(2001, 1, 1), datetime(2002, 2, 1)))
        self.assertEqual(TimeRangeLike.convert([datetime(2001, 1, 1), datetime(2002, 2, 1)]),
                         (datetime(2001, 1, 1), datetime(2002, 2, 1)))
        self.assertEqual(TimeRangeLike.convert('2001-01-01, 2002-01-01'),
                         (datetime(2001, 1, 1), datetime(2002, 1, 1, 23, 59, 59)))
        self.assertEqual(TimeRangeLike.convert('2001-01-01, 2002-01-01'),
                         (datetime(2001, 1, 1), datetime(2002, 1, 1, 23, 59, 59)))

        with self.assertRaises(ValidationError) as err:
            TimeRangeLike.convert('2002-01-01, 2001-01-01')
        self.assertTrue('cannot be converted into a' in str(err.exception))

    def test_format(self):
        self.assertEqual(TimeRangeLike.format(None), '')
        self.assertEqual(TimeRangeLike.format((datetime(2001, 1, 1), datetime(2002, 1, 1))),
                         '2001-01-01, 2002-01-01')
        self.assertEqual(TimeRangeLike.format((datetime(2001, 1, 1, 12), datetime(2002, 1, 1, 9, 30, 2))),
                         '2001-01-01T12:00:00, 2002-01-01T09:30:02')


class TypeNamesTest(TestCase):
    """
    This test fails, if any of the expected type names change.
    We use these type names in cate-desktop to map from type to validators and GUI editors.

    NOTE: If one of these tests fails, we have to change the cate-desktop code w.r.t. the type name change.
    """

    def test_python_primitive_type_names(self):
        """
        Python primitive types
        """
        self.assertEqual(object_to_qualified_name(bool), 'bool')
        self.assertEqual(object_to_qualified_name(int), 'int')
        self.assertEqual(object_to_qualified_name(float), 'float')
        self.assertEqual(object_to_qualified_name(str), 'str')

    def test_cate_cdm_type_names(self):
        """
        Cate Common Data Model (CDM) types
        """
        self.assertEqual(object_to_qualified_name(np.ndarray), 'numpy.ndarray')
        self.assertEqual(object_to_qualified_name(xr.Dataset), 'xarray.core.dataset.Dataset')
        self.assertEqual(object_to_qualified_name(xr.DataArray), 'xarray.core.dataarray.DataArray')
        self.assertEqual(object_to_qualified_name(gpd.GeoDataFrame), 'geopandas.geodataframe.GeoDataFrame')
        self.assertEqual(object_to_qualified_name(gpd.GeoSeries), 'geopandas.geoseries.GeoSeries')
        self.assertEqual(object_to_qualified_name(pd.DataFrame), 'pandas.core.frame.DataFrame')
        self.assertEqual(object_to_qualified_name(pd.Series), 'pandas.core.series.Series')

    def test_cate_op_api_type_names(self):
        """
        Additional Cate types used by operations API.
        """
        self.assertEqual(object_to_qualified_name(VarName), 'cate.core.types.VarName')
        self.assertEqual(object_to_qualified_name(VarNamesLike), 'cate.core.types.VarNamesLike')
        self.assertEqual(object_to_qualified_name(PointLike), 'cate.core.types.PointLike')
        self.assertEqual(object_to_qualified_name(PolygonLike), 'cate.core.types.PolygonLike')
        self.assertEqual(object_to_qualified_name(GeometryLike), 'cate.core.types.GeometryLike')
        self.assertEqual(object_to_qualified_name(TimeRangeLike), 'cate.core.types.TimeRangeLike')


class ArbitraryTest(TestCase):
    def test_convert(self):
        self.assertEqual(Arbitrary.convert(None), None)
        self.assertEqual(Arbitrary.convert(434), 434)
        self.assertEqual(Arbitrary.convert(3.4), 3.4)
        self.assertEqual(Arbitrary.convert(True), True)
        self.assertEqual(Arbitrary.convert((3, 5, 7)), (3, 5, 7))
        self.assertEqual(Arbitrary.convert('abc'), 'abc')

    def test_format(self):
        self.assertEqual(Arbitrary.format(None), '')
        self.assertEqual(Arbitrary.format(434), '434')
        self.assertEqual(Arbitrary.format(3.4), '3.4')
        self.assertEqual(Arbitrary.format("abc"), "abc")
        self.assertEqual(Arbitrary.format(True), 'True')


class LiteralTest(TestCase):
    def test_convert(self):
        self.assertEqual(Literal.convert(''), None)
        self.assertEqual(Literal.convert('None'), None)
        self.assertEqual(Literal.convert('434'), 434)
        self.assertEqual(Literal.convert('3.4'), 3.4)
        self.assertEqual(Literal.convert('True'), True)
        self.assertEqual(Literal.convert('"abc"'), 'abc')
        # Does not work anymore in Python 3.7
        # self.assertEqual(Literal.convert('2 + 6'), 8)
        self.assertEqual(Literal.convert('[3, 5, 7]'), [3, 5, 7])
        self.assertEqual(Literal.convert('(3, 5, 7)'), (3, 5, 7))

        with self.assertRaises(ValidationError):
            Literal.convert('[1,2')
        with self.assertRaises(ValidationError):
            Literal.convert('abc')

    def test_format(self):
        self.assertEqual(Literal.format(None), '')
        self.assertEqual(Literal.format(434), '434')
        self.assertEqual(Literal.format(3.4), '3.4')
        self.assertEqual(Literal.format("abc"), "'abc'")
        self.assertEqual(Literal.format(True), 'True')
        self.assertEqual(Literal.format([1, 2, 3]), '[1, 2, 3]')


class DatasetLikeTest(TestCase):
    def test_convert(self):
        self.assertEqual(DatasetLike.convert(None), None)

        data = {'time': ['2000-01-01', '2000-01-02', '2000-01-03'],
                'c1': [4, 5, 6],
                'c2': [6, 7, 8]}
        pd_ds = pd.DataFrame(data=data)
        pd_ds = pd_ds.set_index('time')
        pd_ds.index = pd.to_datetime(pd_ds.index)
        xr_ds = xr.Dataset(data_vars=data)
        self.assertIsInstance(DatasetLike.convert(xr_ds), xr.Dataset)
        self.assertIsInstance(DatasetLike.convert(pd_ds), xr.Dataset)

        with self.assertRaises(ValidationError):
            DatasetLike.convert(42)

    def test_format(self):
        self.assertEqual(DatasetLike.format(None), '')

        with self.assertRaises(ValidationError):
            data = {'v1': [4, 5, 6], 'v2': [6, 7, 8]}
            DatasetLike.format(xr.Dataset(data_vars=data))


class DataFrameLikeTest(TestCase):
    def test_convert(self):
        self.assertEqual(DataFrameLike.convert(None), None)

        data = {'c1': [4, 5, 6], 'c2': [6, 7, 8]}
        xr_ds = xr.Dataset(data_vars=data)
        pd_ds = pd.DataFrame(data=data)
        gdf_ds = gpd.GeoDataFrame.from_features(read_test_features())
        proxy_gdf_ds = GeoDataFrame.from_features(read_test_features())
        self.assertIsInstance(DataFrameLike.convert(xr_ds), pd.DataFrame)
        self.assertIsInstance(DataFrameLike.convert(pd_ds), pd.DataFrame)
        self.assertIs(DataFrameLike.convert(pd_ds), pd_ds)
        self.assertIsInstance(DataFrameLike.convert(gdf_ds), gpd.GeoDataFrame)
        self.assertIs(DataFrameLike.convert(gdf_ds), gdf_ds)
        self.assertIsInstance(DataFrameLike.convert(proxy_gdf_ds), GeoDataFrame)
        self.assertIs(DataFrameLike.convert(proxy_gdf_ds), proxy_gdf_ds)

        with self.assertRaises(ValidationError):
            DataFrameLike.convert(42)

    def test_format(self):
        self.assertEqual(DataFrameLike.format(None), '')

        with self.assertRaises(ValidationError):
            data = {'c1': [4, 5, 6], 'c2': [6, 7, 8]}
            DataFrameLike.format(pd.DataFrame(data=data))


class TestGeoDataFrame(TestCase):

    def test_compat_with_geopandas(self):
        features = read_test_features()
        gdf = GeoDataFrame.from_features(features)
        self.assertIs(type(gdf), GeoDataFrame)
        self.assertIsInstance(gdf, GeoDataFrame)
        self.assertIsInstance(gdf, gpd.GeoDataFrame)
        self.assertIsInstance(gdf, pd.DataFrame)
        self.assertIs(gdf.features, features)
        self.assertIsInstance(gdf['A'], pd.Series)
        self.assertIsInstance(gdf.geometry, gpd.GeoSeries)

    def test_close(self):
        features = read_test_features()
        gdf = GeoDataFrame.from_features(features)
        self.assertIs(gdf.features, features)
        self.assertIsInstance(gdf.lazy_data_frame, gpd.GeoDataFrame)
        gdf.close()
        self.assertIsNone(gdf.features)
        self.assertIsNone(gdf.lazy_data_frame)

    def test_fat_ops(self):
        features = read_test_features()
        gdf = GeoDataFrame.from_features(features)
        self.assertIsNotNone(gdf.crs)

        from cate.ops.data_frame import data_frame_min, data_frame_max
        df_min = data_frame_min(gdf, 'C')
        self.assertIsInstance(df_min, gpd.GeoDataFrame)
        self.assertEqual(len(df_min), 1)
        # assertCountEqual ignores the order of the list
        self.assertCountEqual(list(df_min.columns), ['A', 'B', 'C', 'geometry'])
        self.assertIsInstance(df_min.geometry, gpd.GeoSeries)
        self.assertIsNotNone(df_min.crs)

        df_max = data_frame_max(gdf, 'C')
        self.assertIsInstance(df_max, gpd.GeoDataFrame)
        self.assertEqual(len(df_max), 1)
        # assertCountEqual ignores the order of the list
        self.assertCountEqual(list(df_max.columns), ['A', 'B', 'C', 'geometry'])
        self.assertIsInstance(df_max.geometry, gpd.GeoSeries)
        self.assertIsNotNone(df_max.crs)


def read_test_features():
    import fiona
    import os
    return fiona.open(os.path.join(os.path.dirname(__file__), 'test_data', 'test.geojson'))
