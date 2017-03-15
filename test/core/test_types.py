from collections import namedtuple
from typing import Union, Tuple
from unittest import TestCase

from cate.core.op import op_input, OpRegistry
from cate.core.types import Like, VariableNamesLike, PointLike, PolygonLike, TimeRangeLike
from cate.util import object_to_qualified_name

from shapely.geometry import Point, Polygon
from datetime import datetime, date

# 'TestPoint' is an example type which may come from Cate API or other required API.
TestPoint = namedtuple('TestPoint', ['x', 'y'])


# 'TestType' represents a type to be used for values that should actually be a 'TestPoint' but
# also have a representation as a `str` such as "2.1,4.3", ar a tuple of two floats (2.1,4.3).
# The "typing type" for this is given by TestType.TYPE.

class TestType(Like[TestPoint]):
    # The "typing type"

    TYPE = Union[TestPoint, Tuple[float, float], str]

    @classmethod
    def convert(cls, value) -> TestPoint:
        try:
            if isinstance(value, TestPoint):
                return value
            if isinstance(value, str):
                pair = value.split(',')
                return TestPoint(float(pair[0]), float(pair[1]))
            return TestPoint(value[0], value[1])
        except Exception:
            raise ValueError('cannot convert value <%s> to %s' % (value, cls.name()))

    @classmethod
    def format(cls, value: TestPoint) -> str:
        return "%s, %s" % (value.x, value.y)


# TestType = NewType('TestType', _TestType)


# 'scale_point' is an example operation that makes use of the TestType type for argument point_like

_OP_REGISTRY = OpRegistry()


@op_input("point_like", data_type=TestType, registry=_OP_REGISTRY)
def scale_point(point_like: TestType.TYPE, factor: float) -> TestPoint:
    point = TestType.convert(point_like)
    return TestPoint(factor * point.x, factor * point.y)


class TestTypeTest(TestCase):
    def test_use(self):
        self.assertEqual(scale_point("2.4, 4.8", 0.5), TestPoint(1.2, 2.4))
        self.assertEqual(scale_point((2.4, 4.8), 0.5), TestPoint(1.2, 2.4))
        self.assertEqual(scale_point(TestPoint(2.4, 4.8), 0.5), TestPoint(1.2, 2.4))

    def test_abuse(self):
        with self.assertRaises(ValueError) as e:
            scale_point("A, 4.8", 0.5)
        self.assertEqual(str(e.exception), "cannot convert value <A, 4.8> to TestType")

        with self.assertRaises(ValueError) as e:
            scale_point(25.1, 0.5)
        self.assertEqual(str(e.exception), "cannot convert value <25.1> to TestType")

    def test_registered_op(self):
        registered_op = _OP_REGISTRY.get_op(object_to_qualified_name(scale_point))
        point = registered_op(point_like="2.4, 4.8", factor=0.5)
        self.assertEqual(point, TestPoint(1.2, 2.4))

    def test_name(self):
        self.assertEqual(TestType.name(), "TestType")

    def test_accepts(self):
        self.assertTrue(TestType.accepts("2.4, 4.8"))
        self.assertTrue(TestType.accepts((2.4, 4.8)))
        self.assertTrue(TestType.accepts([2.4, 4.8]))
        self.assertTrue(TestType.accepts(TestPoint(2.4, 4.8)))
        self.assertFalse(TestType.accepts("A, 4.8"))
        self.assertFalse(TestType.accepts(25.1))

    def test_format(self):
        self.assertEqual(TestType.format(TestPoint(2.4, 4.8)), "2.4, 4.8")


class VariableNamesLikeTest(TestCase):
    """
    Test the VariableNamesLike type
    """
    def test_accepts(self):
        self.assertTrue(VariableNamesLike.accepts('aa'))
        self.assertTrue(VariableNamesLike.accepts('aa,bb,cc'))
        self.assertTrue(VariableNamesLike.accepts(['aa', 'bb', 'cc']))
        self.assertFalse(VariableNamesLike.accepts(1.0))
        self.assertFalse(VariableNamesLike.accepts([1, 2, 4]))
        self.assertFalse(VariableNamesLike.accepts(['aa', 2, 'bb']))

    def test_convert(self):
        expected = ['aa', 'b*', 'cc']
        actual = VariableNamesLike.convert('aa,b*,cc')
        self.assertEqual(actual, expected)

        with self.assertRaises(ValueError) as err:
            VariableNamesLike.convert(['aa', 1, 'bb'])
        print(str(err))
        self.assertTrue('string or a list' in str(err.exception))

    def teset_format(self):
        self.assertEqual(VariableNamesLike.format(['aa', 'bb', 'cc']),
                         "['aa', 'bb', 'cc']")


class PointLikeTest(TestCase):
    """
    Test the PointLike type
    """

    def test_accepts(self):
        self.assertTrue(PointLike.accepts("2.4, 4.8"))
        self.assertTrue(PointLike.accepts((2.4, 4.8)))
        self.assertTrue(PointLike.accepts([2.4, 4.8]))
        self.assertTrue(PointLike.accepts(Point(2.4, 4.8)))
        self.assertFalse(PointLike.accepts("A, 4.8"))
        self.assertFalse(PointLike.accepts(25.1))

    def test_convert(self):
        expected = Point(0.0, 1.0)
        actual = PointLike.convert('0.0,1.0')
        self.assertTrue(expected, actual)

        with self.assertRaises(ValueError) as err:
            PointLike.convert('0.0,abc')
        self.assertTrue('cannot convert' in str(err.exception))

    def test_format(self):
        self.assertEqual(PointLike.format(Point(2.4, 4.8)), "2.4, 4.8")


class PolygonLikeTest(TestCase):
    """
    Test the PolygonLike type
    """
    def test_accepts(self):
        self.assertTrue(PolygonLike.accepts("0.0,0.0,1.1,1.1"))
        self.assertTrue(PolygonLike.accepts("0.0, 0.0, 1.1, 1.1"))

        coords = [(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)]
        pol = Polygon(coords)
        self.assertTrue(PolygonLike.accepts(coords))
        self.assertTrue(PolygonLike.accepts(pol))
        self.assertTrue(PolygonLike.accepts(pol.wkt))

        self.assertFalse(PolygonLike.accepts("0.0,aaa,1.1,1.1"))
        self.assertFalse(PolygonLike.accepts("0.0, aaa, 1.1, 1.1"))

        coords = [(0.0, 0.0), (0.0, 1.0), (1.0, 'aaa'), (1.0, 0.0)]
        self.assertFalse(PolygonLike.accepts(coords))

        coords = [(0.0, 0.0), (0.0, 1.0), 'Guten Morgen, Berlin!', (1.0, 0.0)]
        self.assertFalse(PolygonLike.accepts(coords))

        invalid = Polygon([(0.0, 0.0), (0.0, 0.0), (0.0, 0.0), (0.0, 0.0)])
        self.assertFalse(PolygonLike.accepts(invalid))

        wkt = 'MULTIPOLYGON()'
        self.assertFalse(PolygonLike.accepts(wkt))

        invalid = 'Something_something_in_the_month_of_May'
        self.assertFalse(PolygonLike.accepts(invalid))

        self.assertFalse(PolygonLike.accepts(1.0))

    def test_convert(self):
        coords = [(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)]
        expected = Polygon(coords)
        actual = PolygonLike.convert(coords)
        self.assertTrue(actual, expected)

        with self.assertRaises(ValueError) as err:
            PolygonLike.convert('aaa')
        self.assertTrue('cannot convert' in str(err.exception))

    def test_format(self):
        coords = [(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)]
        pol = PolygonLike.convert(coords)
        self.assertTrue('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))' ==
                        PolygonLike.format(pol))


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
        expected = (datetime(2001, 1, 1), datetime(2002, 1, 1, 23, 59, 59))
        actual = TimeRangeLike.convert('2001-01-01, 2002-01-01')
        self.assertTrue(actual == expected)

        with self.assertRaises(ValueError) as err:
            TimeRangeLike.convert('2002-01-01, 2001-01-01')
        self.assertTrue('cannot convert' in str(err.exception))

    def test_format(self):
        expected = '2001-01-01T00:00:00 2002-01-01T00:00:00'
        actual = TimeRangeLike.format((datetime(2001, 1, 1), datetime(2002, 1, 1)))
        self.assertTrue(expected, actual)
