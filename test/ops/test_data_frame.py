from unittest import TestCase

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely.geometry
import shapely.wkt
from shapely.geometry import Point

from cate.core.types import ValidationError
from cate.core.types import GeoDataFrameProxy
from cate.ops.data_frame import data_frame_min, data_frame_max, data_frame_query, data_frame_find_closest, \
    great_circle_distance, data_frame_aggregate, data_frame_subset

test_point = 'POINT (597842.4375881671 5519903.13366397)'

test_poly_4326 = 'POLYGON ((-80 -40, -70 -40, ' \
                                  '-70 -45, -80 -45, ' \
                                  '-80 -40))'


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

    gdf_32718 = gpd.GeoDataFrame({'A': [1]},
                                 crs={'init': 'epsg:32718'},
                                 geometry=[shapely.wkt.loads(test_point)])

    test_region_4326 = shapely.wkt.loads(test_poly_4326)

    gdfp = GeoDataFrameProxy.from_features(gdf.__geo_interface__['features'])

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
        self._test_data_frame_query_with_geom(TestDataFrameOps.gdf)
        self._test_data_frame_query_with_geom(TestDataFrameOps.gdfp)

    def _test_data_frame_query_with_geom(self, gdf):
        df2 = data_frame_query(gdf, "not C and @almost_equals('10,10')")
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 1)
        df2 = data_frame_query(gdf, "not C and @contains('10,10')")
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 1)
        df2 = data_frame_query(gdf, "not C and @crosses('10,10')")
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 0)
        df2 = data_frame_query(gdf, "not C and @disjoint('10,10')")
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 2)
        df2 = data_frame_query(gdf, "not C and @intersects('19, 9, 21, 31')")
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 1)
        df2 = data_frame_query(gdf, "not C and @touches('10, 10, 20, 30')")
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 3)
        df2 = data_frame_query(gdf, "@within('19, 9, 21, 31')")
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
        df2 = data_frame_query(gdf, "not C and @within('19, 9, 21, 31')")
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 1)
        self.assertEqual(list(df2.columns), ['A', 'B', 'C', 'D', 'geometry'])
        self.assertEqual(df2.iloc[0, 0], 4)
        self.assertEqual(df2.iloc[0, 1], 'x')
        self.assertEqual(df2.iloc[0, 2], False)
        self.assertEqual(df2.iloc[0, 3], 0.3)
        df2 = data_frame_query(gdf, "not C and geometry.within(@from_wkt('19, 9, 21, 31'))")
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 1)
        self.assertEqual(list(df2.columns), ['A', 'B', 'C', 'D', 'geometry'])
        self.assertEqual(df2.iloc[0, 0], 4)
        self.assertEqual(df2.iloc[0, 1], 'x')
        self.assertEqual(df2.iloc[0, 2], False)
        self.assertEqual(df2.iloc[0, 3], 0.3)

        df2 = data_frame_query(TestDataFrameOps.gdf_32718, "@within('" + test_poly_4326 + "')")
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 1)

    def test_data_frame_subset(self):
        df2 = data_frame_subset(TestDataFrameOps.gdf,
                                region='POLYGON((-10 0, 25 0, 25 30, -10 0))')
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 3)
        self.assertIn('A', df2)
        self.assertIn('B', df2)
        self.assertIn('C', df2)
        self.assertIn('D', df2)
        self.assertIn('geometry', df2)

        df2 = data_frame_subset(TestDataFrameOps.gdf,
                                var_names="A,C",
                                region='POLYGON((-10 0, 25 0, 25 30, -10 0))')
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 3)
        self.assertIn('A', df2)
        self.assertNotIn('B', df2)
        self.assertIn('C', df2)
        self.assertNotIn('D', df2)
        self.assertIn('geometry', df2)

        df2 = data_frame_subset(TestDataFrameOps.gdf,
                                var_names="A,C",
                                region='POLYGON((30 30, 40 30, 40 40, 30 30))')
        self.assertIsInstance(df2, gpd.GeoDataFrame)
        self.assertEqual(len(df2), 0)

        df2 = data_frame_subset(TestDataFrameOps.gdf_32718,
                                var_names='A',
                                region=TestDataFrameOps.test_region_4326)
        self.assertEqual(len(df2), 1)

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

    def test_data_frame_aggregate(self):
        # Generate mock data
        data = {'name': ['A', 'B', 'C'],
                'lat': [45, 46, 47.5],
                'lon': [-120, -121.2, -122.9]}

        df = pd.DataFrame(data)
        # needs to be a copy
        gdf_empty_geo = gpd.GeoDataFrame(df).copy()
        gdf = gpd.GeoDataFrame(df, geometry=[Point(xy) for xy in zip(df['lon'], df['lat'])])

        var_names_not_agg = 'name, lat, lon'
        var_names_not_in = 'asdc, lat, lon'
        var_names_valid = ['lat', 'lon']
        aggregations = ["count", "mean", "median", "sum", "std", "min", "max"]

        # Assert that a Validation exception is thrown if the df is None
        with self.assertRaises(ValidationError):
            data_frame_aggregate(df=None)

        # Assert that a Validation exception is thrown if the var_names contain non-existing fields in the df
        with self.assertRaises(ValidationError):
            data_frame_aggregate(df=df, var_names=var_names_not_in)

        # Assert that a Validation exception is thrown if the var_names contain non-aggregatable fields
        with self.assertRaises(ValidationError):
            data_frame_aggregate(df=df, var_names=var_names_not_agg)

        # Assert that a Validation exception is thrown if the GeoDataFrame does not have a geometry
        with self.assertRaises(ValidationError):
            data_frame_aggregate(df=gdf_empty_geo, var_names=None)

        with self.assertRaises(ValidationError):
            data_frame_aggregate(df=gdf_empty_geo, var_names='lat')

        # assert that a input and output types for df are the same
        rdf = data_frame_aggregate(df=gdf, var_names=var_names_valid)
        self.assertEqual(len(rdf), 1)

        # assert that columns are return if var_names = None for a DataFrame
        rdf = data_frame_aggregate(df=df, var_names=None)
        self.assertEqual(len(rdf.columns), len(aggregations) * len(var_names_valid))

        # assert that columns are return if var_names = None for a GeoDataFrame
        rdf = data_frame_aggregate(df=gdf, var_names=None, aggregate_geometry=True)
        self.assertEqual(len(rdf.columns), len(aggregations) * len(var_names_valid) + 1)

        # assert that geometry union is created
        rdf = data_frame_aggregate(df=gdf, var_names=var_names_valid, aggregate_geometry=True)
        self.assertIsNotNone(rdf.geometry)


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
