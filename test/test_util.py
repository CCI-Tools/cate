from unittest import TestCase
from xml.etree.ElementTree import ElementTree

from ect.core.util import object_to_qualified_name, qualified_name_to_object


class UtilTest(TestCase):
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
