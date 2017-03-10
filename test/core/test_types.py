from unittest import TestCase

from cate.core import types
from typing import Union, List, NewType


class TestTypes(TestCase):
    def test_interface(self):
        """
        Test the overall complex types interface
        """
        DOESNTEXIST = 'DOESNTEXIST'
        AliasedType = Union[str, List[str]]
        MyType = NewType('MyType', Union[str, List[str]])
        is_type = types.is_type

        with self.assertRaises(TypeError) as err:
            is_type('aa', 1.0)
        self.assertTrue('Not possible' in str(err.exception))

        with self.assertRaises(TypeError) as err:
            is_type('aa', DOESNTEXIST)
        self.assertTrue('DOESNTEXIST' in str(err.exception))

        with self.assertRaises(TypeError) as err:
            is_type('aa', Union[str, List[str]])
        self.assertTrue('Union[str,' in str(err.exception))

        with self.assertRaises(TypeError) as err:
            is_type('aa', AliasedType)
        self.assertTrue('Union[str,' in str(err.exception))

        with self.assertRaises(TypeError) as err:
            is_type('aa', MyType)
        self.assertTrue('Not possible' in str(err.exception))


        self.assertTrue(is_type('aa', types.VARIABLE))

    def test_variable_type(self):
        """
        Test the VARIABLE type
        """
        VARIABLE = types.VARIABLE
        is_type = types.is_type
        to_op_object = types.to_op_object

        self.assertTrue(is_type('aa', VARIABLE))
        self.assertTrue(is_type('aa,bb,cc', VARIABLE))
        self.assertTrue(is_type(['aa', 'bb', 'vv'], VARIABLE))
        self.assertFalse(is_type(1.0, VARIABLE))
        self.assertFalse(is_type([1, 2, 4], VARIABLE))
        self.assertFalse(is_type(['aa', 2, 'bb'], VARIABLE))

        expected = ['aa', 'bb', 'cc']
        actual = to_op_object('aa,bb,cc', VARIABLE)
        self.assertEqual(actual, expected)

        with self.assertRaises(TypeError) as err:
            to_op_object(['aa', 1, 'bb'], VARIABLE)
        self.assertTrue('Provided value' in str(err.exception))
