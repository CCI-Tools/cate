from collections import namedtuple
from datetime import datetime, date
from typing import Union, Tuple
from unittest import TestCase

from shapely.geometry import Point, Polygon

from cate.core.op import op_input, OpRegistry
from cate.core.types import Like, VarNamesLike, VarName, PointLike, PolygonLike, TimeRangeLike, GeometryLike, DictLike, \
    TimeLike
from cate.util import object_to_qualified_name, OrderedDict

import xarray as xr
import pandas as pd
import geopandas as gpd
import numpy as np


# 'ExamplePoint' is an example type which may come from Cate API or other required API.
ExamplePoint = namedtuple('ExamplePoint', ['x', 'y'])


# 'ExampleType' represents a type to be used for values that should actually be a 'ExampleType' but
# also have a representation as a `str` such as "2.1,4.3", ar a tuple of two floats (2.1,4.3).
# The "typing type" for this is given by ExampleType.TYPE.

class ExampleType(Like[ExamplePoint]):
    # The "typing type"

    TYPE = Union[ExamplePoint, Tuple[float, float], str]

    @classmethod
    def convert(cls, value) -> ExamplePoint:
        try:
            if isinstance(value, ExamplePoint):
                return value
            if isinstance(value, str):
                pair = value.split(',')
                return ExamplePoint(float(pair[0]), float(pair[1]))
            return ExamplePoint(value[0], value[1])
        except Exception:
            raise ValueError('cannot convert value <%s> to %s' % (value, cls.name()))

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
        with self.assertRaises(ValueError) as e:
            scale_point("A, 4.8", 0.5)
        self.assertEqual(str(e.exception), "cannot convert value <A, 4.8> to ExampleType")

        with self.assertRaises(ValueError) as e:
            scale_point(25.1, 0.5)
        self.assertEqual(str(e.exception), "cannot convert value <25.1> to ExampleType")

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

        with self.assertRaises(ValueError) as err:
            VarNamesLike.convert(['aa', 1, 'bb'])
        self.assertTrue('string or a list' in str(err.exception))
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

        with self.assertRaises(ValueError) as err:
            VarName.convert(['aa', 'bb', 'cc'])
        self.assertTrue('cannot convert' in str(err.exception))
        self.assertEqual(None, VarName.convert(None))

    def test_format(self):
        self.assertEqual('aa', VarName.format('aa'))


class DictLikeTest(TestCase):
    """
    Test the DictLike type
    """

    def test_accepts(self):
        self.assertTrue(DictLike.accepts(None))
        self.assertTrue(DictLike.accepts(''))
        self.assertTrue(DictLike.accepts(' '))
        self.assertTrue(DictLike.accepts('a=6, b=5.3, c=True, d="Hello"'))

        self.assertFalse(DictLike.accepts('{a=True}'))
        self.assertFalse(DictLike.accepts('a=true'))
        self.assertFalse(DictLike.accepts('{a, b}'))
        self.assertFalse(DictLike.accepts(['aa', 'bb', 'cc']))
        self.assertFalse(DictLike.accepts(1.0))

    def test_convert(self):
        self.assertEqual(DictLike.convert(None), None)
        self.assertEqual(DictLike.convert('  '), None)
        self.assertEqual(DictLike.convert('name="bibo", thres=0.5, drop=False'), dict(name="bibo", thres=0.5, drop=False))

        with self.assertRaises(ValueError) as err:
            DictLike.convert('{a=8, b}')
        self.assertEqual(str(err.exception), 'cannot convert value <{a=8, b}> to DictLike')

    def test_format(self):
        self.assertEqual(DictLike.format(OrderedDict([('name', 'bibo'), ('thres', 0.5), ('drop', True)])),
                         "name='bibo', thres=0.5, drop=True")


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
        self.assertFalse(PointLike.accepts("A, 4.8"))
        self.assertFalse(PointLike.accepts(25.1))

    def test_convert(self):
        self.assertEqual(PointLike.convert(None), None)
        self.assertEqual(PointLike.convert(''), None)
        self.assertEqual(PointLike.convert('0.0,1.0'), Point(0.0, 1.0))
        with self.assertRaises(ValueError) as err:
            PointLike.convert('0.0,abc')
        self.assertEqual(str(err.exception), 'cannot convert value <0.0,abc> to PointLike')

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

        coords = [(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)]
        pol = Polygon(coords)
        self.assertTrue(PolygonLike.accepts(coords))
        self.assertTrue(PolygonLike.accepts(pol))
        self.assertTrue(PolygonLike.accepts(pol.wkt))

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
        coords = [(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)]
        self.assertTrue(PolygonLike.convert(coords), Polygon(coords))

        with self.assertRaises(ValueError) as err:
            PolygonLike.convert('aaa')
        self.assertEqual(str(err.exception), 'cannot convert value <aaa> to PolygonLike')

    def test_format(self):
        self.assertEqual(PolygonLike.format(None), '')
        coords = [(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)]
        pol = PolygonLike.convert(coords)
        self.assertEqual(PolygonLike.format(pol), 'POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))')


class GeometryLikeTest(TestCase):
    """
    Test the PolygonLike type
    """

    def test_accepts(self):
        self.assertTrue(GeometryLike.accepts("0.0,0.0,1.1,1.1"))
        self.assertTrue(GeometryLike.accepts("0.0, 0.0, 1.1, 1.1"))

        coords = [(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)]
        pol = Polygon(coords)
        self.assertTrue(GeometryLike.accepts(coords))
        self.assertTrue(GeometryLike.accepts(pol))
        self.assertTrue(GeometryLike.accepts(pol.wkt))

        self.assertFalse(GeometryLike.accepts("0.0,aaa,1.1,1.1"))
        self.assertFalse(GeometryLike.accepts("0.0, aaa, 1.1, 1.1"))

        # coords = [(0.0, 0.0), (0.0, 1.0), (1.0, 'aaa'), (1.0, 0.0)]
        # self.assertFalse(GeometryLike.accepts(coords))

        # coords = [(0.0, 0.0), (0.0, 1.0), 'Guten Morgen, Berlin!', (1.0, 0.0)]
        # self.assertFalse(GeometryLike.accepts(coords))

        invalid = Polygon([(0.0, 0.0), (0.0, 0.0), (0.0, 0.0), (0.0, 0.0)])
        self.assertFalse(GeometryLike.accepts(invalid))

        self.assertFalse(GeometryLike.accepts('MULTIPOLYGON()'))
        self.assertFalse(GeometryLike.accepts('Something_in_the_month_of_May'))
        self.assertFalse(GeometryLike.accepts(1.0))

    def test_convert(self):
        self.assertEqual(GeometryLike.convert(None), None)
        self.assertEqual(GeometryLike.convert(""), None)
        coords = [(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)]
        self.assertTrue(GeometryLike.convert(coords), Polygon(coords))

        with self.assertRaises(ValueError) as err:
            GeometryLike.convert('aaa')
        self.assertEqual(str(err.exception), 'cannot convert value <aaa> to GeometryLike')

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
        self.assertEqual(TimeRangeLike.convert(''), None)
        self.assertEqual(TimeRangeLike.convert('2001-01-01, 2002-01-01'),
                         (datetime(2001, 1, 1), datetime(2002, 1, 1, 23, 59, 59)))

        with self.assertRaises(ValueError) as err:
            TimeRangeLike.convert('2002-01-01, 2001-01-01')
        self.assertTrue('cannot convert' in str(err.exception))

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
