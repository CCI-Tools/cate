from unittest import TestCase

import fiona
import pandas as pd
import geopandas as gpd

from cate.ops.fat import fat_min, fat_max


class TestFAT(TestCase):
    df = pd.DataFrame({'A': [1, 2, 3], 'B': ['x', 'y', 'z'], 'C': [True, False, True], 'D': [0.4, 0.5, 0.3]})

    def test_fat_min(self):
        df2 = fat_min(TestFAT.df, 'A')
        self.assertEqual(len(df2), 1)
        self.assertEqual(list(df2.columns), ['A', 'B', 'C', 'D'])
        self.assertEqual(df2.iloc[0, 0], 1)
        self.assertEqual(df2.iloc[0, 1], 'x')
        self.assertEqual(df2.iloc[0, 2], True)
        self.assertEqual(df2.iloc[0, 3], 0.4)

    def test_fat_max(self):
        df2 = fat_max(TestFAT.df, 'D')
        self.assertEqual(len(df2), 1)
        self.assertEqual(list(df2.columns), ['A', 'B', 'C', 'D'])
        self.assertEqual(df2.iloc[0, 0], 2)
        self.assertEqual(df2.iloc[0, 1], 'y')
        self.assertEqual(df2.iloc[0, 2], False)
        self.assertEqual(df2.iloc[0, 3], 0.5)


