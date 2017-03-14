from collections import namedtuple
from typing import Union, Tuple
from unittest import TestCase

from cate.core.op import op_input, OpRegistry
from cate.core.types import Like, Variable, PointLike
from cate.util import object_to_qualified_name

# 'Point' is an example type which may come from Cate API or other required API.
Point = namedtuple('Point', ['x', 'y'])


# 'TestType' represents a type to be used for values that should actually be a 'Point' but
# also have a representation as a `str` such as "2.1,4.3", ar a tuple of two floats (2.1,4.3).
# The "typing type" for this is given by TestType.TYPE.

class TestType(Like[Point]):
    # The "typing type"

    TYPE = Union[Point, Tuple[float, float], str]

    @classmethod
    def convert(cls, value) -> Point:
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


# TestType = NewType('TestType', _TestType)


# 'scale_point' is an example operation that makes use of the TestType type for argument point_like

_OP_REGISTRY = OpRegistry()


@op_input("point_like", data_type=TestType, registry=_OP_REGISTRY)
def scale_point(point_like: TestType.TYPE, factor: float) -> Point:
    point = TestType.convert(point_like)
    return Point(factor * point.x, factor * point.y)


class TestTypeTest(TestCase):
    def test_use(self):
        self.assertEqual(scale_point("2.4, 4.8", 0.5), Point(1.2, 2.4))
        self.assertEqual(scale_point((2.4, 4.8), 0.5), Point(1.2, 2.4))
        self.assertEqual(scale_point(Point(2.4, 4.8), 0.5), Point(1.2, 2.4))

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
        self.assertEqual(point, Point(1.2, 2.4))

    def test_name(self):
        self.assertEqual(TestType.name(), "TestType")

    def test_accepts(self):
        self.assertTrue(TestType.accepts("2.4, 4.8"))
        self.assertTrue(TestType.accepts((2.4, 4.8)))
        self.assertTrue(TestType.accepts([2.4, 4.8]))
        self.assertTrue(TestType.accepts(Point(2.4, 4.8)))
        self.assertFalse(TestType.accepts("A, 4.8"))
        self.assertFalse(TestType.accepts(25.1))

    def test_format(self):
        self.assertEqual(TestType.format(Point(2.4, 4.8)), "2.4, 4.8")


class VariableTest(TestCase):
    """
    Test the Variable type
    """
    def test_accepts(self):
        self.assertTrue(Variable.accepts('aa'))
        self.assertTrue(Variable.accepts('aa,bb,cc'))
        self.assertTrue(Variable.accepts(['aa', 'bb', 'cc']))
        self.assertFalse(Variable.accepts(1.0))
        self.assertFalse(Variable.accepts([1, 2, 4]))
        self.assertFalse(Variable.accepts(['aa', 2, 'bb']))

    def test_convert(self):
        expected = ['aa', 'b*', 'cc']
        actual = Variable.convert('aa,b*,cc')
        self.assertEqual(actual, expected)

        with self.assertRaises(ValueError) as err:
            Variable.convert(['aa', 1, 'bb'])
        print(str(err))
        self.assertTrue('string or a list' in str(err.exception))

    def teset_format(self):
        self.assertEqual(Variable.format(['aa', 'bb', 'cc']),
                         "['aa', 'bb', 'cc']")


class PointLikeTest(TestCase):
    """
    Test the PointLike type
    """
    from shapely.geometry import Point

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
