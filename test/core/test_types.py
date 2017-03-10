from abc import abstractclassmethod
from collections import namedtuple
from typing import Union, Tuple, Any, Generic, TypeVar
from unittest import TestCase

# Note, this is experimental code!

T = TypeVar('T')


class Like(Generic[T]):
    TYPE = None

    def __init__(self):
        raise NotImplementedError('%s cannot be instantiated' % self.__class__)

    @classmethod
    def name(cls) -> str:
        return cls.__name__

    @classmethod
    def accepts(cls, value: Any) -> bool:
        try:
            cls.convert(value)
            return True
        except ValueError:
            return False

    @abstractclassmethod
    def convert(cls, value: Any) -> T:
        pass

    @classmethod
    def format(cls, value: T) -> str:
        return str(value)


# 'Point' is an example type which may come from Cate API or other required API.
Point = namedtuple('Point', ['x', 'y'])


# 'PointLike' represents a type to be used for values that should actually be a 'Point' but
# also have a representation as a `str` such as "2.1,4.3", ar a tuple of two floats (2.1,4.3).
# The "typing type" for this is given by PointLike.TYPE.

class PointLike(Like[Point]):
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


# PointLike = NewType('PointLike', _PointLike)


# 'scale_point' is an example operation that makes use of the PointLike type for argument point_like

def scale_point(point_like: PointLike.TYPE, factor: float) -> Point:
    point = PointLike.convert(point_like)
    return Point(factor * point.x, factor * point.y)


class PointLikeTest(TestCase):
    def test_use(self):
        self.assertEqual(scale_point("2.4, 4.8", 0.5), Point(1.2, 2.4))
        self.assertEqual(scale_point((2.4, 4.8), 0.5), Point(1.2, 2.4))
        self.assertEqual(scale_point(Point(2.4, 4.8), 0.5), Point(1.2, 2.4))

    def test_abuse(self):
        with self.assertRaises(ValueError) as e:
            scale_point("A, 4.8", 0.5)
        self.assertEqual(str(e.exception), "cannot convert value <A, 4.8> to PointLike")

        with self.assertRaises(ValueError) as e:
            scale_point(25.1, 0.5)
        self.assertEqual(str(e.exception), "cannot convert value <25.1> to PointLike")

    def test_new(self):
        with self.assertRaises(NotImplementedError) as e:
            PointLike()
        self.assertEqual(str(e.exception), "test.core.test_types.PointLike cannot be instantiated")

    def test_name(self):
        self.assertEqual(PointLike.name(), "PointLike")

    def test_accepts(self):
        self.assertTrue(PointLike.accepts("2.4, 4.8"))
        self.assertTrue(PointLike.accepts((2.4, 4.8)))
        self.assertTrue(PointLike.accepts([2.4, 4.8]))
        self.assertTrue(PointLike.accepts(Point(2.4, 4.8)))
        self.assertFalse(PointLike.accepts("A, 4.8"))
        self.assertFalse(PointLike.accepts(25.1))

    def test_format(self):
        self.assertEqual(PointLike.format(Point(2.4, 4.8)), "2.4, 4.8")
