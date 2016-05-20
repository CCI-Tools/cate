from unittest import TestCase
from xml.etree.ElementTree import ElementTree

from ect.core.util import extend
from ect.core.util import object_to_qualified_name, qualified_name_to_object


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
        with self.assertRaises(ValueError):
            object_to_qualified_name({}, fail=True)

    def test_qualified_name_to_object(self):
        self.assertIs(qualified_name_to_object('float'), float)
        self.assertIs(qualified_name_to_object('builtins.float'), float)
        self.assertIs(qualified_name_to_object('unittest.case.TestCase'), TestCase)
        self.assertIs(qualified_name_to_object('xml.etree.ElementTree.ElementTree'), ElementTree)
        with self.assertRaises(ImportError):
            qualified_name_to_object('numpi.ndarray')
        with self.assertRaises(AttributeError):
            qualified_name_to_object('flaot')
