from collections import OrderedDict
from datetime import datetime, date
from unittest import TestCase
from xml.etree.ElementTree import ElementTree
from numpy import dtype

from cate.util.misc import encode_url_path
from cate.util.misc import object_to_qualified_name, qualified_name_to_object
from cate.util.misc import to_datetime, to_datetime_range
from cate.util.misc import to_list
from cate.util.misc import to_str_constant, is_str_constant


# noinspection PyUnresolvedReferences
class UtilTest(TestCase):
    def test_object_to_qualified_name(self):
        self.assertEqual(object_to_qualified_name(float), 'float')
        self.assertEqual(object_to_qualified_name(dtype('float64')), 'float64')
        self.assertEqual(object_to_qualified_name(TestCase), 'unittest.case.TestCase')
        self.assertEqual(object_to_qualified_name(ElementTree), 'xml.etree.ElementTree.ElementTree')
        self.assertEqual(object_to_qualified_name({}, fail=False), '{}')
        with self.assertRaisesRegex(ValueError, "missing attribute '__name__'"):
            object_to_qualified_name({}, fail=True)

    def test_qualified_name_to_object(self):
        self.assertIs(qualified_name_to_object('float'), float)
        self.assertIs(qualified_name_to_object('builtins.float'), float)
        self.assertIs(qualified_name_to_object('unittest.case.TestCase'), TestCase)
        self.assertIs(qualified_name_to_object('xml.etree.ElementTree.ElementTree'), ElementTree)
        with self.assertRaisesRegex(ImportError, "No module named 'numpi'"):
            qualified_name_to_object('numpi.ndarray')
        with self.assertRaisesRegex(AttributeError, "module 'builtins' has no attribute 'flaot'"):
            qualified_name_to_object('flaot')

    def test_encode_path(self):
        self.assertEqual(encode_url_path('/ws/init',
                                         query_args=OrderedDict([('base_path', '/home/norman/workpaces'),
                                                                 ('description', 'Hi there!')])),
                         '/ws/init?base_path=%2Fhome%2Fnorman%2Fworkpaces&description=Hi+there%21')
        self.assertEqual(encode_url_path('/ws/init',
                                         query_args=OrderedDict([('base_path', 'C:\\Users\\Norman\\workpaces'),
                                                                 ('description', 'Hi there!')])),
                         '/ws/init?base_path=C%3A%5CUsers%5CNorman%5Cworkpaces&description=Hi+there%21')

        self.assertEqual(encode_url_path('/ws/get/{base_path}',
                                         path_args=dict(base_path='/home/norman/workpaces')),
                         '/ws/get/%2Fhome%2Fnorman%2Fworkpaces')
        self.assertEqual(encode_url_path('/ws/get/{base_path}',
                                         path_args=dict(base_path='C:\\Users\\Norman\\workpaces')),
                         '/ws/get/C%3A%5CUsers%5CNorman%5Cworkpaces')

    def test_to_datetime(self):
        dt = to_datetime('1998-11-20 10:14:08', default=None)
        self.assertIsInstance(dt, datetime)
        self.assertEqual(datetime(1998, 11, 20, 10, 14, 8), dt)

        dt = to_datetime('2001-01-01', default=None)
        self.assertIsInstance(dt, datetime)
        self.assertEqual(datetime(2001, 1, 1), dt)

        dt = to_datetime('2002-03', default=None)
        self.assertIsInstance(dt, datetime)
        self.assertEqual(datetime(2002, 3, 1), dt)

        dt = to_datetime('2003', default=None)
        self.assertIsInstance(dt, datetime)
        self.assertEqual(datetime(2003, 1, 1), dt)

        dt = to_datetime('1998-11-20 10:14:08', upper_bound=True, default=None)
        self.assertIsInstance(dt, datetime)
        self.assertEqual(datetime(1998, 11, 20, 10, 14, 8), dt)

        dt = to_datetime('2001-01-01', upper_bound=True, default=None)
        self.assertIsInstance(dt, datetime)
        self.assertEqual(datetime(2001, 1, 1, 23, 59, 59), dt)

        dt = to_datetime('2002-03', upper_bound=True, default=None)
        self.assertIsInstance(dt, datetime)
        self.assertEqual(datetime(2002, 3, 28, 23, 59, 59), dt)

        dt = to_datetime('2003', upper_bound=True, default=None)
        self.assertIsInstance(dt, datetime)
        self.assertEqual(datetime(2003, 12, 31, 23, 59, 59), dt)

        dt = to_datetime('2001-01-01 2:3:5', default=None)
        self.assertIsInstance(dt, datetime)
        self.assertEqual(datetime(2001, 1, 1, 2, 3, 5), dt)

        dt = to_datetime(datetime(2001, 1, 1), default=None)
        self.assertIsInstance(dt, datetime)
        self.assertEqual(datetime(2001, 1, 1), dt)

        dt = to_datetime(date(2012, 4, 20), default=None)
        self.assertIsInstance(dt, datetime)
        self.assertEqual(datetime(2012, 4, 20, 12), dt)

        dt = to_datetime(None, default=datetime(2001, 1, 1))
        self.assertIsInstance(dt, datetime)
        self.assertEqual(datetime(2001, 1, 1), dt)

        with self.assertRaises(TypeError):
            # noinspection PyTypeChecker
            to_datetime(1, default=None)
        with self.assertRaises(ValueError):
            to_datetime("42", default=None)

    def test_to_datetime_range(self):
        dtr = to_datetime_range(None, '')
        self.assertIsNone(dtr)

        dtr = to_datetime_range('2008', None)
        self.assertEqual(dtr[0], datetime(2008, 1, 1))
        self.assertEqual(dtr[1], datetime(2008, 12, 30, 23, 59, 59))

        dtr = to_datetime_range('2008-08', None)
        self.assertEqual(dtr[0], datetime(2008, 8, 1))
        self.assertEqual(dtr[1], datetime(2008, 8, 28, 23, 59, 59))

        dtr = to_datetime_range('2008-10-10', None)
        self.assertEqual(dtr[0], datetime(2008, 10, 10, 0, 0))
        self.assertEqual(dtr[1], datetime(2008, 10, 10, 23, 59, 59))

        with self.assertRaises(ValueError):
            to_datetime_range("211", "2012")


class ToListTest(TestCase):
    def test_none_and_empty(self):
        self.assertEqual(to_list(None), None)
        self.assertEqual(to_list([]), [])

    def test_str(self):
        self.assertEqual(to_list('a'), ['a'])
        self.assertEqual(to_list('a, b, c'), ['a', 'b', 'c'])
        self.assertEqual(to_list(['a', 'b', 'c']), ['a', 'b', 'c'])
        self.assertEqual(to_list(('a', 'b', 'c')), ['a', 'b', 'c'])
        self.assertEqual(to_list([1, 2, 3]), ['1', '2', '3'])

    def test_int(self):
        self.assertEqual(to_list(1, dtype=int), [1])
        self.assertEqual(to_list('1, 2, 3', dtype=int), [1, 2, 3])
        self.assertEqual(to_list([1, 2, 3], dtype=int), [1, 2, 3])
        self.assertEqual(to_list((1, 2, 3), dtype=int), [1, 2, 3])
        self.assertEqual(to_list(['1', '2', '3'], dtype=int), [1, 2, 3])


class StrConstantTest(TestCase):
    def test_to_str_constant(self):
        self.assertEqual(to_str_constant('abc'), "'abc'")
        self.assertEqual(to_str_constant('abc', "'"), "'abc'")
        self.assertEqual(to_str_constant('abc', '"'), '"abc"')
        self.assertEqual(to_str_constant("a'bc", "'"), "'a\\'bc'")
        self.assertEqual(to_str_constant("a'bc", '"'), '"a\'bc"')
        self.assertEqual(to_str_constant('a"bc', '"'), '"a\\"bc"')
        self.assertEqual(to_str_constant('a"bc', "'"), "'a\"bc'")

    def test_to_str_constant_with_eval(self):
        s1 = '\\\''
        s2 = to_str_constant(s1, "'")
        self.assertEqual(s2, "'\\\\\\''")
        self.assertEqual(eval(s2), s1)
        s2 = to_str_constant(s1, '"')
        self.assertEqual(s2, '"\\\\\'"')
        self.assertEqual(eval(s2), s1)

    def test_is_str_constant(self):
        self.assertEqual(is_str_constant('abc'), False)
        self.assertEqual(is_str_constant('\\\''), False)
        self.assertEqual(is_str_constant('"abc"'), True)
        self.assertEqual(is_str_constant('"abc\''), False)
        self.assertEqual(is_str_constant("'abc'"), True)
        self.assertEqual(is_str_constant("\"abc'"), False)
