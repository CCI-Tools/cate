from collections import OrderedDict
from datetime import datetime, date
from unittest import TestCase
from xml.etree.ElementTree import ElementTree

from ect.core.util import Namespace
from ect.core.util import UNDEFINED
from ect.core.util import encode_url_path
from ect.core.util import extend
from ect.core.util import object_to_qualified_name, qualified_name_to_object
from ect.core.util import to_datetime, to_datetime_range
from ect.core.util import to_list


class UndefinedTest(TestCase):
    def test_it(self):
        self.assertIsNotNone(UNDEFINED)
        self.assertEqual(str(UNDEFINED), 'UNDEFINED')
        self.assertEqual(repr(UNDEFINED), 'UNDEFINED')


# noinspection PyUnusedLocal
class NamespaceTest(TestCase):
    def test_empty(self):
        namespace = Namespace()
        self.assertEqual(len(namespace), 0)
        self.assertEqual(str(namespace), 'Namespace()')
        self.assertEqual(repr(namespace), 'Namespace()')
        self.assertFalse('a' in namespace)
        self.assertEqual(list(namespace), [])
        with self.assertRaisesRegex(KeyError, "'a'"):
            v = namespace['a']
        with self.assertRaisesRegex(AttributeError, "attribute 'a' not found"):
            v = namespace.a
        with self.assertRaisesRegex(IndexError, "list index out of range"):
            v = namespace[0]
        with self.assertRaisesRegex(IndexError, "list index out of range"):
            namespace[0] = True

    def test_set_items(self):
        namespace = Namespace()
        namespace['z'] = 10
        namespace.a = 20
        namespace.p = 30
        self.assertEqual(len(namespace), 3)
        self.assertEqual(namespace['z'], 10)
        self.assertEqual(namespace['a'], 20)
        self.assertEqual(namespace['p'], 30)
        self.assertEqual(namespace.z, 10)
        self.assertEqual(namespace.a, 20)
        self.assertEqual(namespace.p, 30)
        self.assertEqual(namespace[0], 10)
        self.assertEqual(namespace[1], 20)
        self.assertEqual(namespace[2], 30)
        self.assertEqual(namespace[:], [10, 20, 30])
        self.assertEqual(list(namespace), [('z', 10), ('a', 20), ('p', 30)])
        del namespace.a
        self.assertEqual(len(namespace), 2)
        self.assertEqual(namespace['z'], 10)
        self.assertEqual(namespace['p'], 30)
        self.assertEqual(namespace.z, 10)
        self.assertEqual(namespace.p, 30)
        self.assertEqual(namespace[0], 10)
        self.assertEqual(namespace[1], 30)
        self.assertEqual(namespace[:], [10, 30])
        self.assertEqual(list(namespace), [('z', 10), ('p', 30)])
        del namespace[0]
        self.assertEqual(len(namespace), 1)
        self.assertEqual(namespace['p'], 30)
        self.assertEqual(namespace.p, 30)
        self.assertEqual(namespace[0], 30)
        self.assertEqual(namespace[:], [30])
        self.assertEqual(list(namespace), [('p', 30)])
        del namespace['p']
        self.assertEqual(len(namespace), 0)
        self.assertEqual(namespace[:], [])
        self.assertEqual(list(namespace), [])

    def test_non_empty(self):
        namespace = Namespace([('a', 10), ('b', 20), ('c', 30)])
        self.assertEqual(len(namespace), 3)
        self.assertEqual(str(namespace), "Namespace([('a', 10), ('b', 20), ('c', 30)])")
        self.assertEqual(repr(namespace), "Namespace([('a', 10), ('b', 20), ('c', 30)])")
        self.assertTrue('a' in namespace)
        self.assertTrue('b' in namespace)
        self.assertTrue('c' in namespace)
        self.assertEqual(namespace['a'], 10)
        self.assertEqual(namespace['b'], 20)
        self.assertEqual(namespace['c'], 30)
        self.assertEqual(namespace.a, 10)
        self.assertEqual(namespace.b, 20)
        self.assertEqual(namespace.c, 30)
        self.assertEqual(namespace[0], 10)
        self.assertEqual(namespace[1], 20)
        self.assertEqual(namespace[2], 30)
        self.assertEqual(list(namespace), [('a', 10), ('b', 20), ('c', 30)])

    def test_for_in(self):
        namespace = Namespace([('a', 10), ('b', 20), ('c', 30)])
        items = [(name, value) for name, value in namespace]
        self.assertEqual(items, [('a', 10), ('b', 20), ('c', 30)])

    def test_to_dict(self):
        namespace = Namespace([('a', 10), ('b', 20), ('c', 30)])
        self.assertEqual(OrderedDict(namespace), OrderedDict([('a', 10), ('b', 20), ('c', 30)]))
        self.assertEqual(dict(namespace), {'a': 10, 'b': 20, 'c': 30})


# noinspection PyUnresolvedReferences
class UtilTest(TestCase):
    def test_extension_property(self):
        # noinspection PyMethodMayBeStatic
        class Api:
            def m1(self, x):
                return 2 * x

        @extend(Api)
        class MyApiExt:
            """My API class extension"""

            def __init__(self, api0):
                self.api = api0

            def m2(self, x):
                return self.api.m1(x) + 2

        self.assertTrue(hasattr(Api, 'my_api_ext'))
        api = Api()
        self.assertEqual(api.my_api_ext.m2(8), 2 * 8 + 2)
        self.assertEqual(api.my_api_ext.__doc__, "My API class extension")

    def test_object_to_qualified_name(self):
        self.assertEqual(object_to_qualified_name(float), 'float')
        self.assertEqual(object_to_qualified_name(TestCase), 'unittest.case.TestCase')
        self.assertEqual(object_to_qualified_name(ElementTree), 'xml.etree.ElementTree.ElementTree')
        self.assertIs(object_to_qualified_name({}, fail=False), None)
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
