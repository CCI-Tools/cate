from unittest import TestCase

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely.geometry
import shapely.wkt
from shapely.geometry import Point

from cate.ops.data_frame import data_frame_min, data_frame_max, data_frame_query, data_frame_find_closest, \
    great_circle_distance


class TestDataFrameOps(TestCase):
    df = pd.DataFrame({'A': [1, 2, 3, 4, 5, 6],
                       'B': ['a', 'b', 'c', 'x', 'y', 'z'],
                       'C': [False, False, True, False, True, True],
                       'D': [0.4, 0.5, 0.3, 0.3, 0.1, 0.4]})

    gdf = gpd.GeoDataFrame({'A': [1, 2, 3, 4, 5, 6],
                            'B': ['a', 'b', 'c', 'x', 'y', 'z'],
                            'C': [False, False, True, False, True, True],
                            'D': [0.4, 0.5, 0.3, 0.3, 0.1, 0.4],
                            'geometry': gpd.GeoSeries([
                                shapely.wkt.loads('POINT(10 10)'),
                                shapely.wkt.loads('POINT(10 20)'),
                                shapely.wkt.loads('POINT(10 30)'),
                                shapely.wkt.loads('POINT(20 30)'),
                                shapely.wkt.loads('POINT(20 20)'),
                                shapely.wkt.loads('POINT(20 10)'),
                            ])})

    def test_data_frame_min(self):
        df2 = data_frame_min(TestDataFrameOps.df, 'D')
        self.assertIsInstance(df2, pd.DataFrame)
        self.assertEqual(len(df2), 1)
        self.assertEqual(list(df2.columns), ['A', 'B', 'C', 'D'])
        self.assertEqual(df2.iloc[0, 0], 5)
        self.assertEqual(df2.iloc[0, 1], 'y')
        self.assertEqual(df2.iloc[0, 2], True)
        self.assertEqual(df2.iloc[0, 3], 0.1)

    def test_data_frame_max(self):
        df2 = data_frame_max(TestDataFrameOps.df, 'D')
        self.assertIsInstance(df2, pd.DataFrame)
        self.assertEqual(len(df2), 1)
        self.assertEqual(list(df2.columns), ['A', 'B', 'C', 'D'])
        self.assertEqual(df2.iloc[0, 0], 2)
        self.assertEqual(df2.iloc[0, 1], 'b')
        self.assertEqual(df2.iloc[0, 2], False)
        self.assertEqual(df2.iloc[0, 3], 0.5)

    def test_data_frame_query(self):
        df2 = data_frame_query(TestDataFrameOps.df, "D >= 0.4 and B != 'b'")
        self.assertIsInstance(df2, pd.DataFrame)
        self.assertEqual(len(df2), 2)
        self.assertEqual(list(df2.columns), ['A', 'B', 'C', 'D'])
        self.assertEqual(df2.iloc[0, 0], 1)
        self.assertEqual(df2.iloc[0, 1], 'a')
        self.assertEqual(df2.iloc[0, 2], False)
        self.assertEqual(df2.iloc[0, 3], 0.4)
        self.assertEqual(df2.iloc[1, 0], 6)
        self.assertEqual(df2.iloc[1, 1], 'z')
        self.assertEqual(df2.iloc[1, 2], True)
        self.assertEqual(df2.iloc[1, 3], 0.4)

    def test_data_frame_query_with_geom(self):
        df2 = data_frame_query(TestDataFrameOps.gdf, "not C and @almost_equals('10,10')")
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 1)

        df2 = data_frame_query(TestDataFrameOps.gdf, "not C and @contains('10,10')")
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 1)

        df2 = data_frame_query(TestDataFrameOps.gdf, "not C and @crosses('10,10')")
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 0)

        df2 = data_frame_query(TestDataFrameOps.gdf, "not C and @disjoint('10,10')")
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 2)

        df2 = data_frame_query(TestDataFrameOps.gdf, "not C and @intersects('19, 9, 21, 31')")
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 1)

        df2 = data_frame_query(TestDataFrameOps.gdf, "not C and @touches('10, 10, 20, 30')")
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 3)

        df2 = data_frame_query(TestDataFrameOps.gdf, "@within('19, 9, 21, 31')")
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 3)
        self.assertEqual(list(df2.columns), ['A', 'B', 'C', 'D', 'geometry'])
        self.assertEqual(df2.iloc[0, 0], 4)
        self.assertEqual(df2.iloc[1, 0], 5)
        self.assertEqual(df2.iloc[2, 0], 6)
        self.assertEqual(df2.iloc[0, 1], 'x')
        self.assertEqual(df2.iloc[1, 1], 'y')
        self.assertEqual(df2.iloc[2, 1], 'z')
        self.assertEqual(df2.iloc[0, 2], False)
        self.assertEqual(df2.iloc[1, 2], True)
        self.assertEqual(df2.iloc[2, 2], True)
        self.assertEqual(df2.iloc[0, 3], 0.3)
        self.assertEqual(df2.iloc[1, 3], 0.1)
        self.assertEqual(df2.iloc[2, 3], 0.4)

        df2 = data_frame_query(TestDataFrameOps.gdf, "not C and @within('19, 9, 21, 31')")
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 1)
        self.assertEqual(list(df2.columns), ['A', 'B', 'C', 'D', 'geometry'])
        self.assertEqual(df2.iloc[0, 0], 4)
        self.assertEqual(df2.iloc[0, 1], 'x')
        self.assertEqual(df2.iloc[0, 2], False)
        self.assertEqual(df2.iloc[0, 3], 0.3)

        df2 = data_frame_query(TestDataFrameOps.gdf, "not C and geometry.within(@from_wkt('19, 9, 21, 31'))")
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 1)
        self.assertEqual(list(df2.columns), ['A', 'B', 'C', 'D', 'geometry'])
        self.assertEqual(df2.iloc[0, 0], 4)
        self.assertEqual(df2.iloc[0, 1], 'x')
        self.assertEqual(df2.iloc[0, 2], False)
        self.assertEqual(df2.iloc[0, 3], 0.3)

    def test_data_frame_find_closest(self):
        df2 = data_frame_find_closest(TestDataFrameOps.gdf, 'POINT(20 30)',
                                      dist_col_name='dist')
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 1)
        self.assertIn('A', df2)
        self.assertIn('B', df2)
        self.assertIn('C', df2)
        self.assertIn('D', df2)
        self.assertIn('geometry', df2)
        self.assertIn('dist', df2)
        self.assertEqual(1, len(df2['dist']))
        self.assertEqual(0.0, df2['dist'].iloc[0])
        self.assertEqual(shapely.wkt.loads('POINT(20 30)'), df2['geometry'].iloc[0])

        df2 = data_frame_find_closest(TestDataFrameOps.gdf, 'POINT(21 28)',
                                      max_results=3, dist_col_name='dist')
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 3)
        self.assertIn('A', df2)
        self.assertIn('B', df2)
        self.assertIn('C', df2)
        self.assertIn('D', df2)
        self.assertIn('geometry', df2)
        self.assertIn('dist', df2)
        np.testing.assert_approx_equal(2.1828435, df2['dist'].iloc[0])
        np.testing.assert_approx_equal(8.0518568, df2['dist'].iloc[1])
        np.testing.assert_approx_equal(9.8221713, df2['dist'].iloc[2])
        self.assertEqual(shapely.wkt.loads('POINT(20 30)'), df2['geometry'].iloc[0])
        self.assertEqual(shapely.wkt.loads('POINT(20 20)'), df2['geometry'].iloc[1])
        self.assertEqual(shapely.wkt.loads('POINT(10 30)'), df2['geometry'].iloc[2])

        df2 = data_frame_find_closest(TestDataFrameOps.gdf, 'POINT(21 28)',
                                      max_dist=9.0, max_results=3, dist_col_name='dist')
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 2)
        self.assertIn('A', df2)
        self.assertIn('B', df2)
        self.assertIn('C', df2)
        self.assertIn('D', df2)
        self.assertIn('geometry', df2)
        self.assertIn('dist', df2)
        np.testing.assert_approx_equal(2.1828435, df2['dist'].iloc[0])
        np.testing.assert_approx_equal(8.0518568, df2['dist'].iloc[1])
        self.assertEqual(shapely.wkt.loads('POINT(20 30)'), df2['geometry'].iloc[0])
        self.assertEqual(shapely.wkt.loads('POINT(20 20)'), df2['geometry'].iloc[1])

class GreatCircleDistanceTest(TestCase):
    def test_great_circle_distance(self):
        dist = great_circle_distance(Point(20, 20), Point(20, 20))
        self.assertIsNotNone(dist)
        np.testing.assert_approx_equal(0.0, dist)
        dist = great_circle_distance(Point(20, 0), Point(20, 30))
        np.testing.assert_approx_equal(30.0, dist)
        dist = great_circle_distance(Point(-20, 0), Point(20, 0))
        np.testing.assert_approx_equal(40.0, dist)
        dist = great_circle_distance(Point(-155, 0), Point(155, 0))
        np.testing.assert_approx_equal(50.0, dist)
        dist = great_circle_distance(Point(0, 0), Point(0, 90))
        np.testing.assert_approx_equal(90.0, dist)
        dist = great_circle_distance(Point(0, -90), Point(0, 90))
        np.testing.assert_approx_equal(180.0, dist)
        dist = great_circle_distance(Point(0, 180), Point(0, 0))
        np.testing.assert_approx_equal(180.0, dist)
        dist = great_circle_distance(Point(0, 0), Point(1, 1))
        np.testing.assert_approx_equal(1.4141777, dist)
