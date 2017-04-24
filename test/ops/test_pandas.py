"""
Tests for pandas wrappers
"""

from unittest import TestCase

import pandas as pd
import numpy as np

from cate.ops import pandas_fillna
from cate.core.op import OP_REGISTRY
from cate.util.misc import object_to_qualified_name


class TestFillna(TestCase):
    """
    Test fillna operation
    """
    def test_nominal(self):
        """
        Test nominal operation
        """
        # Test na filling using a given method
        data = {'A': [1, 2, 3, np.nan, 4, 9, np.nan, np.nan, 1, 0, 4, 6],
                'B': [5, 6, 8, 7, 5, np.nan, np.nan, np.nan, 1, 2, 7, 6]}
        expected = {'A': [1, 2, 3, 3, 4, 9, 9, 9, 1, 0, 4, 6],
                    'B': [5, 6, 8, 7, 5, 5, 5, 5, 1, 2, 7, 6]}
        time = pd.date_range('2000-01-01', freq='MS', periods=12)

        expected = pd.DataFrame(data=expected, index=time, dtype=float)
        df = pd.DataFrame(data=data, index=time, dtype=float)

        actual = pandas_fillna(df, method='ffill')
        self.assertTrue(actual.equals(expected))

        # Test na filling using a given value
        actual = pandas_fillna(df, value=3.14)
        expected = {'A': [1, 2, 3, 3.14, 4, 9, 3.14, 3.14, 1, 0, 4, 6],
                    'B': [5, 6, 8, 7, 5, 3.14, 3.14, 3.14, 1, 2, 7, 6]}
        expected = pd.DataFrame(data=expected, index=time, dtype=float)
        self.assertTrue(actual.equals(expected))

    def test_registered(self):
        """
        Test operation when run as a registered operation
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(pandas_fillna))
        # Test na filling using a given method
        data = {'A': [1, 2, 3, np.nan, 4, 9, np.nan, np.nan, 1, 0, 4, 6],
                'B': [5, 6, 8, 7, 5, np.nan, np.nan, np.nan, 1, 2, 7, 6]}
        expected = {'A': [1, 2, 3, 3, 4, 9, 9, 9, 1, 0, 4, 6],
                    'B': [5, 6, 8, 7, 5, 5, 5, 5, 1, 2, 7, 6]}
        time = pd.date_range('2000-01-01', freq='MS', periods=12)

        expected = pd.DataFrame(data=expected, index=time, dtype=float)
        df = pd.DataFrame(data=data, index=time, dtype=float)

        actual = reg_op(df=df, method='ffill')
        self.assertTrue(actual.equals(expected))
