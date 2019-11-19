from collections import OrderedDict
from datetime import datetime, date
from unittest import TestCase
from xml.etree.ElementTree import ElementTree

import numpy as np

from cate.util.misc import encode_url_path, to_json, to_scalar
from cate.util.misc import object_to_qualified_name, qualified_name_to_object
from cate.util.misc import to_datetime, to_datetime_range
from cate.util.misc import to_list
from cate.util.misc import to_str_constant, is_str_constant
from cate.util.misc import new_indexed_name
from cate.util.undefined import UNDEFINED


class UtilTest(TestCase):
    def test_object_to_qualified_name(self):
        self.assertEqual(object_to_qualified_name(float), 'float')
        self.assertEqual(object_to_qualified_name(np.dtype('float64')), 'float64')
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

    def test_new_indexed_name(self):
        with self.assertRaises(ValueError) as cm:
            new_indexed_name(['var_1', 'var_2'], 'ds_{id}')
        self.assertEqual(str(cm.exception), 'pattern must contain "{index}"')

        with self.assertRaises(ValueError) as cm:
            new_indexed_name(['var_1', 'var_2'], '{index}_plot')
        self.assertEqual(str(cm.exception), 'pattern does not yield a valid name')

        self.assertEqual(new_indexed_name(['var_1', 'res_4', 'var_2'], 'ds_{index}'), 'ds_1')
        self.assertEqual(new_indexed_name(['var_1', 'res_4', 'var_2'], 'var_{index}'), 'var_3')
        self.assertEqual(new_indexed_name(['var_1', 'res_4', 'var_2'], 'res_{index}'), 'res_5')
        self.assertEqual(new_indexed_name(['var_1', 'res_4', 'var_2_subs'], 'var_{index}_subs'), 'var_3_subs')
        self.assertEqual(new_indexed_name(['var_1', 'res_4', 'var_4_3'], 'var_{index}_{index}'), 'var_5_5')

        self.assertEqual(new_indexed_name([], 'ds_{index}'), 'ds_1')
        self.assertEqual(new_indexed_name(['ds5'], 'ds_{index}'), 'ds_1')
        self.assertEqual(new_indexed_name(['ds_005'], 'ds_{index}'), 'ds_6')


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


class ToJsonTest(TestCase):
    def test_none_and_empty(self):
        self.assertEqual(to_json(None), None)
        self.assertEqual(to_json([]), [])
        self.assertEqual(to_json({}), {})
        self.assertEqual(to_json(''), '')

    def test_numpy(self):
        self.assertEqual(to_json(np.array([])),
                         [])
        self.assertEqual(to_json(np.array([1])),
                         [1])
        self.assertEqual(to_json(np.array([1, 2, 3])),
                         [1, 2, 3])
        self.assertEqual(to_json(np.array([[1, 2], [3, 4]])),
                         [[1, 2], [3, 4]])
        self.assertEqual(to_json(np.array([1, 2, 3])[2]),
                         3)
        self.assertEqual(to_json(np.array([np.datetime64('2005-02-21'),
                                           np.datetime64('2005-02-23'),
                                           np.datetime64('2005-02-25')])),
                         ['2005-02-21',
                          '2005-02-23',
                          '2005-02-25'])
        self.assertEqual(to_json(np.array([np.datetime64('2005-02-21')])), ['2005-02-21'])
        self.assertEqual(to_json(np.ndarray),
                         'numpy.ndarray')

    def test_types(self):
        self.assertEqual(to_json(str), "str")
        self.assertEqual(to_json(OrderedDict), "collections.OrderedDict")

    def test_scalar_values(self):
        self.assertEqual(to_json(True), True)
        self.assertEqual(to_json(False), False)
        self.assertEqual(to_json('ohoh'), 'ohoh')
        self.assertEqual(to_json(234), 234)
        self.assertEqual(to_json(4.6), 4.6)
        self.assertEqual(to_json(np.array(np.datetime64('2005-02-21'))), '2005-02-21')

    def test_composite_values(self):
        self.assertEqual(to_json([1, 'no!', 3]), [1, 'no!', 3])
        self.assertEqual(to_json({"a": [1, 2, 3], 6: 'b'}), {"a": [1, 2, 3], "6": 'b'})


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


class ToScalarTest(TestCase):
    def test_primitives(self):
        self.assertEqual(to_scalar(3456), 3456)
        self.assertEqual(to_scalar(34.56789), 34.56789)
        self.assertEqual(to_scalar(34.56789, ndigits=1), 34.6)
        self.assertEqual(to_scalar(True), True)
        self.assertEqual(to_scalar("Oh!"), "Oh!")
        self.assertEqual(to_scalar("Oh!Oh!", nchars=2), "Oh...")
        self.assertEqual(to_scalar(None), None)

    def test_list(self):
        self.assertIs(to_scalar([]), UNDEFINED)
        self.assertEqual(to_scalar([1]), UNDEFINED)
        self.assertIs(to_scalar([1, 2, 3]), UNDEFINED)
        self.assertEqual(to_scalar(["Oh!"]), UNDEFINED)
        self.assertEqual(to_scalar([1, 2, 3], stringify=True), '[1, 2, 3]')

    def test_dict(self):
        self.assertIs(to_scalar({}), UNDEFINED)
        self.assertIs(to_scalar({'a': 1}), UNDEFINED)
        self.assertIs(to_scalar({'a': 1, 'b': 2}), UNDEFINED)
        self.assertEqual(to_scalar({'a': 1, 'b': 2}, stringify=True), "{'a': 1, 'b': 2}")

    def test_ndarrays(self):
        self.assertIs(to_scalar(np.array([])), UNDEFINED)
        self.assertEqual(to_scalar(np.array(234)), 234)
        self.assertEqual(to_scalar(np.array([234])), 234)
        self.assertEqual(to_scalar(np.array([[234]])), 234)
        self.assertIs(to_scalar(np.array([234, 567])), UNDEFINED)
        self.assertIs(to_scalar(np.array([[234], [567]])), UNDEFINED)
        self.assertEqual(to_scalar(np.array([234.567])), 234.567)
        self.assertEqual(to_scalar(np.array(234.567)), 234.567)
        self.assertEqual(to_scalar(np.array([[234.567]])), 234.567)
        self.assertEqual(to_scalar(np.array([234.567, 567.234])), UNDEFINED)
        self.assertEqual(to_scalar(np.array([234.567]), ndigits=2), 234.57)
        self.assertEqual(to_scalar(np.array(True)), True)
        self.assertEqual(to_scalar(np.array([True])), True)
        self.assertIs(to_scalar(np.array([True, False])), UNDEFINED)
        self.assertIs(to_scalar(np.array([[True], [False]])), UNDEFINED)
        self.assertIs(to_scalar(np.array([None])), UNDEFINED)

    def test_xarrays(self):
        try:
            import xarray as xr
            self.assertIs(to_scalar(xr.DataArray(np.array([]))), UNDEFINED)
            self.assertEqual(to_scalar(xr.DataArray(np.array(234))), 234)
            self.assertEqual(to_scalar(xr.DataArray(np.array(234))), 234)
            self.assertEqual(to_scalar(xr.DataArray(np.array([234]))), 234)
            self.assertEqual(to_scalar(xr.DataArray(np.array([[234]]))), 234)
            self.assertIs(to_scalar(xr.DataArray(np.array([234, 567]))), UNDEFINED)
            self.assertIs(to_scalar(xr.DataArray(np.array([[234], [567]]))), UNDEFINED)
            self.assertEqual(to_scalar(xr.DataArray(np.array(234.567))), 234.567)
            self.assertEqual(to_scalar(xr.DataArray(np.array([234.567]))), 234.567)
            self.assertEqual(to_scalar(xr.DataArray(np.array([[234.567]]))), 234.567)
            self.assertEqual(to_scalar(xr.DataArray(np.array([234.567, 567.234]))), UNDEFINED)
            self.assertEqual(to_scalar(xr.DataArray(np.array([234.567])), ndigits=2), 234.57)
            self.assertEqual(to_scalar(xr.DataArray(np.array(True))), True)
            self.assertEqual(to_scalar(xr.DataArray(np.array([True]))), True)
            self.assertIs(to_scalar(xr.DataArray(np.array([True, False]))), UNDEFINED)
            self.assertIs(to_scalar(xr.DataArray(np.array([[True], [False]]))), UNDEFINED)
        except ImportError:
            pass

    def test_pandas(self):
        try:
            import pandas as pd
            self.assertIs(to_scalar(pd.Series(np.array([]))), UNDEFINED)
            self.assertEqual(to_scalar(pd.Series(np.array([234]))), 234)
            self.assertIs(to_scalar(pd.Series(np.array([234, 567]))), UNDEFINED)
            self.assertEqual(to_scalar(pd.Series(np.array([234.567]))), 234.567)
            self.assertEqual(to_scalar(pd.Series(np.array([234.567, 567.234]))), UNDEFINED)
            self.assertEqual(to_scalar(pd.Series(np.array([234.567])), ndigits=2), 234.57)
            self.assertEqual(to_scalar(pd.Series(np.array([True]))), True)
            self.assertIs(to_scalar(pd.Series(np.array([True, False]))), UNDEFINED)

            self.assertIs(to_scalar(pd.Series([])), UNDEFINED)
            self.assertEqual(to_scalar(pd.Series(234)), 234)
            self.assertEqual(to_scalar(pd.Series([234])), 234)
            self.assertIs(to_scalar(pd.Series([234, 567])), UNDEFINED)
            self.assertEqual(to_scalar(pd.Series(234.567)), 234.567)
            self.assertEqual(to_scalar(pd.Series([234.567])), 234.567)
            self.assertEqual(to_scalar(pd.Series([234.567, 567.234])), UNDEFINED)
            self.assertEqual(to_scalar(pd.Series([234.567]), ndigits=2), 234.57)
            self.assertEqual(to_scalar(pd.Series([True])), True)
            self.assertIs(to_scalar(pd.Series([True, False])), UNDEFINED)
        except ImportError:
            pass
