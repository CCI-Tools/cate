from unittest import TestCase
from xml.etree.ElementTree import ElementTree

from ect.core.util import Attributes
from ect.core.util import extend
from ect.core.util import object_to_qualified_name, qualified_name_to_object


class AttributesTest(TestCase):
    def test_empty(self):
        attrs = Attributes()
        self.assertEqual(len(attrs), 0)
        self.assertEqual(str(attrs), 'Attributes()')
        self.assertEqual(repr(attrs), 'Attributes()')
        self.assertFalse('a' in attrs)
        self.assertEqual(list(attrs), [])
        with self.assertRaisesRegex(KeyError, "'a'"):
            v = attrs['a']
        with self.assertRaisesRegex(AttributeError, "attribute 'a' not found"):
            v = attrs.a
        with self.assertRaisesRegex(IndexError, "list index out of range"):
            v = attrs[0]
        with self.assertRaisesRegex(IndexError, "list index out of range"):
            attrs[0] = True

    def test_set_items(self):
        attrs = Attributes()
        attrs['z'] = 10
        attrs.a = 20
        attrs.p = 30
        self.assertEqual(len(attrs), 3)
        self.assertEqual(attrs['z'], 10)
        self.assertEqual(attrs['a'], 20)
        self.assertEqual(attrs['p'], 30)
        self.assertEqual(attrs.z, 10)
        self.assertEqual(attrs.a, 20)
        self.assertEqual(attrs.p, 30)
        self.assertEqual(attrs[0], 10)
        self.assertEqual(attrs[1], 20)
        self.assertEqual(attrs[2], 30)
        self.assertEqual(attrs[:], [10, 20, 30])
        self.assertEqual(list(attrs), [('z', 10), ('a', 20), ('p', 30)])
        del attrs.a
        self.assertEqual(len(attrs), 2)
        self.assertEqual(attrs['z'], 10)
        self.assertEqual(attrs['p'], 30)
        self.assertEqual(attrs.z, 10)
        self.assertEqual(attrs.p, 30)
        self.assertEqual(attrs[0], 10)
        self.assertEqual(attrs[1], 30)
        self.assertEqual(attrs[:], [10, 30])
        self.assertEqual(list(attrs), [('z', 10), ('p', 30)])
        del attrs[0]
        self.assertEqual(len(attrs), 1)
        self.assertEqual(attrs['p'], 30)
        self.assertEqual(attrs.p, 30)
        self.assertEqual(attrs[0], 30)
        self.assertEqual(attrs[:], [30])
        self.assertEqual(list(attrs), [('p', 30)])
        del attrs['p']
        self.assertEqual(len(attrs), 0)
        self.assertEqual(attrs[:], [])
        self.assertEqual(list(attrs), [])

    def test_non_empty(self):
        attrs = Attributes([('a', 10), ('b', 20), ('c', 30)])
        self.assertEqual(len(attrs), 3)
        self.assertEqual(str(attrs), "Attributes([('a', 10), ('b', 20), ('c', 30)])")
        self.assertEqual(repr(attrs), "Attributes([('a', 10), ('b', 20), ('c', 30)])")
        self.assertTrue('a' in attrs)
        self.assertTrue('b' in attrs)
        self.assertTrue('c' in attrs)
        self.assertEqual(attrs['a'], 10)
        self.assertEqual(attrs['b'], 20)
        self.assertEqual(attrs['c'], 30)
        self.assertEqual(attrs.a, 10)
        self.assertEqual(attrs.b, 20)
        self.assertEqual(attrs.c, 30)
        self.assertEqual(attrs[0], 10)
        self.assertEqual(attrs[1], 20)
        self.assertEqual(attrs[2], 30)
        self.assertEqual(list(attrs), [('a', 10), ('b', 20), ('c', 30)])


class UtilTest(TestCase):
    def test_extension_property(self):
        class Api:
            def m1(self, x):
                return 2 * x

        @extend(Api)
        class MyApiExt:
            """My API class extension"""

            def __init__(self, api):
                self.api = api

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
